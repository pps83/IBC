// This file is part of IBC.
// Copyright (C) 2004 Steven M. Kearns (skearns23@yahoo.com )
// Copyright (C) 2004 - 2025 Richard L King (rlking@aultan.com)
// For conditions of distribution and use, see copyright notice in COPYING.txt

// IBC is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// IBC is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with IBC.  If not, see <http://www.gnu.org/licenses/>.

package ibcalpha.ibc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// Owns the lifecycle of the event-channel stream. Separates two concepts:
//   - replayable snapshot state: only READY and LOGGED_OUT are durable enough to
//     replay to a late subscriber. Other tokens are transient and never become
//     STATE.
//   - transient broadcasts: EVENT-only lines (CONNECTION_FAILED, RESTARTING) and
//     the terminal EVENT CLOSING <reason> sequence.
// Subscribers are removed automatically when their channel closes, when a push
// write fails, or when beginClosing tears the whole subscriber set down.
final class EventBroadcaster {

    enum SubscribeResult { Accepted, ServerClosing, AlreadyRegistered, WriteFailed }

    private static final EventBroadcaster _instance = new EventBroadcaster();

    // Off-monitor close for failed subscribers; broadcast() holds the broadcaster monitor.
    private static final ExecutorService _subscriberCloser = Executors.newSingleThreadExecutor(r -> {
        final Thread t = new Thread(r, "IBC-SubscriberCloser");
        t.setDaemon(true);
        return t;
    });

    private final CopyOnWriteArrayList<CommandChannel> mSubscribers = new CopyOnWriteArrayList<>();
    // null = no STATE replay yet. Updated only by transitionState (READY / LOGGED_OUT),
    // never by transient broadcasters; that prevents invalid replays like STATE RESTARTING.
    private volatile String mSnapshotState;
    // Set by beginClosing. Once true, registerSubscriber refuses new connections and
    // emit* calls become no-ops; the terminal EVENT CLOSING line has already been sent.
    private volatile boolean mClosing;
    private volatile long mLastLoggedOutMs;
    // Set on autorestart confirmation; the JVM shutdown hook reads this to decide which CLOSING reason to emit.
    private volatile boolean mGatewayAutoRestartArmed = false;
    private volatile String mGatewayAutoRestartCloseReason = "RESTART";
    private final Object mShutdownHookLock = new Object();
    private boolean mShutdownHookInstalled = false;

    static EventBroadcaster instance() {
        return _instance;
    }

    private EventBroadcaster() {
    }

    // Steady-state transitions: update snapshot AND broadcast EVENT.
    synchronized void emitReady() {
        transitionState("READY");
    }

    // LOGGED_OUT only matters after the Gateway actually became ready. Without this
    // gate, the AbstractLoginHandler trigger fires on initial boot (the login frame
    // always opens during normal startup) and poisons STATE LOGGED_OUT into the
    // snapshot, breaking adoption of healthy Gateway instances. Once logged out,
    // repeat login-frame openings are still useful transition observations.
    synchronized void emitLoggedOut() {
        if (mClosing) return;
        if (!"READY".equals(mSnapshotState) && !"LOGGED_OUT".equals(mSnapshotState)) return;
        final long now = System.currentTimeMillis();
        if ("LOGGED_OUT".equals(mSnapshotState) && now - mLastLoggedOutMs < LOGGED_OUT_DEDUPE_MS) return;
        mLastLoggedOutMs = now;
        transitionState("LOGGED_OUT");
    }

    private static final long LOGGED_OUT_DEDUPE_MS = 30_000L;

    // Transient events: broadcast EVENT only, snapshot untouched. A late subscriber
    // must never see "STATE CONNECTION_FAILED" / "STATE RESTARTING" replayed.
    synchronized void emitConnectionFailed() {
        emitTransient("CONNECTION_FAILED");
    }

    synchronized void emitRestarting() {
        emitTransient("RESTARTING");
    }

    String gatewayAutoRestartCloseReason() {
        if (!mGatewayAutoRestartArmed) {
            return "SHUTDOWN";
        }
        // armGatewayAutoRestart writes the reason before setting the volatile
        // armed flag, so observing armed=true also observes the paired reason.
        return mGatewayAutoRestartCloseReason;
    }

    // Installs (once) a JVM shutdown hook that emits CLOSING if the armed flag is still set at exit.
    void armGatewayAutoRestart(String closeReason) {
        mGatewayAutoRestartCloseReason = (closeReason == null || closeReason.isEmpty()) ? "RESTART" : closeReason;
        mGatewayAutoRestartArmed = true;
        synchronized (mShutdownHookLock) {
            if (mShutdownHookInstalled) return;
            mShutdownHookInstalled = true;
        }
        final Thread hook = new Thread(() -> {
            if (mGatewayAutoRestartArmed) {
                beginClosing(mGatewayAutoRestartCloseReason);
            }
        }, "IBC-AutoRestartCloser");
        try {
            Runtime.getRuntime().addShutdownHook(hook);
        } catch (IllegalStateException ignored) {
            // JVM is already shutting down; beginClosing has presumably already run.
        }
    }

    // Terminal subscriber shutdown sequence:
    //   1. mark broadcaster closing (rejects future register and emit calls)
    //   2. snapshot the subscriber set and clear the live list under the monitor
    //   3. close the command-server listener so no new connection can race in
    //   4. push "EVENT CLOSING <reason>" and close each subscriber socket OUTSIDE the
    //      monitor, in parallel, with a hard total wall-clock budget so one stuck
    //      subscriber cannot delay overall shutdown.
    // Idempotent; subsequent calls are no-ops.
    void beginClosing(String reason) {
        final List<CommandChannel> snapshot;
        final String closeReason;
        final CommandServer server;
        synchronized (this) {
            if (mClosing) return;
            mClosing = true;
            closeReason = reason;
            server = CommandServer.commandServer();

            snapshot = new ArrayList<>(mSubscribers);
            mSubscribers.clear();
        }

        // Listener close before CLOSING so racing late connections fail to connect
        // rather than receiving an EVENT CLOSING immediately followed by EOF. Keep
        // it outside the broadcaster monitor so command-server teardown cannot
        // deadlock against subscriber removal or later shutdown changes.
        if (server != null) {
            server.shutdown();
        }

        if (snapshot.isEmpty()) return;

        final String line = (closeReason == null || closeReason.isEmpty())
            ? "EVENT CLOSING"
            : "EVENT CLOSING " + closeReason;
        final int closerThreads = Math.max(1, Math.min(8, snapshot.size()));
        final ExecutorService closer = Executors.newFixedThreadPool(closerThreads, r -> {
            final Thread t = new Thread(r, "IBC-ClosingFlusher");
            t.setDaemon(true);
            return t;
        });
        for (CommandChannel ch : snapshot) {
            closer.submit(() -> {
                ch.writeClosingAndStop(line, CLOSING_PER_SUBSCRIBER_BUDGET_MS);
                ch.close();
            });
        }
        closer.shutdown();
        try {
            // Total shutdown budget regardless of subscriber count. The flushers run
            // in parallel, so this caps the entire terminal sequence even if every
            // subscriber is stuck on an unresponsive socket.
            closer.awaitTermination(CLOSING_TOTAL_BUDGET_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closer.shutdownNow();
        for (CommandChannel ch : snapshot) {
            ch.close();
        }
    }

    private static final long CLOSING_PER_SUBSCRIBER_BUDGET_MS = 200L;
    private static final long CLOSING_TOTAL_BUDGET_MS          = 500L;

    // Atomic subscribe registration. The subscribe ACK and optional STATE snapshot
    // are queued while holding the broadcaster monitor, but not waited on. That
    // preserves ordering with a concurrent terminal CLOSING without letting one
    // slow socket stall lifecycle event publication under this monitor. Returns:
    //   Accepted          - ACK/snapshot queued, future events will be broadcast.
    //   ServerClosing     - broadcaster has begun closing; caller should write ERROR
    //                       and close.
    //   AlreadyRegistered - this exact channel was already a subscriber.
    //   WriteFailed       - ACK or snapshot could not be queued; channel was
    //                       removed and should be closed.
    synchronized SubscribeResult registerSubscriber(CommandChannel channel) {
        if (channel == null || mClosing) return SubscribeResult.ServerClosing;
        if (!mSubscribers.addIfAbsent(channel)) return SubscribeResult.AlreadyRegistered;
        channel.markSubscribed();
        if (!channel.queueSubscribeAccepted("subscribed", mSnapshotState)) {
            mSubscribers.remove(channel);
            return SubscribeResult.WriteFailed;
        }
        return SubscribeResult.Accepted;
    }

    void removeSubscriber(CommandChannel channel) {
        if (channel != null) {
            mSubscribers.remove(channel);
        }
    }

    // Both helpers run under the broadcaster monitor; callers already hold it.
    private void transitionState(String state) {
        if (mClosing) return;
        mSnapshotState = state;
        broadcast("EVENT " + state);
    }

    private void emitTransient(String token) {
        if (mClosing) return;
        broadcast("EVENT " + token);
    }

    private void broadcast(String line) {
        for (CommandChannel ch : mSubscribers) {
            if (!ch.pushEvent(line)) {
                mSubscribers.remove(ch);
                _subscriberCloser.submit(ch::close);
            }
        }
    }
}

// This file is part of IBC.
// Copyright (C) 2004 Steven M. Kearns (skearns23@yahoo.com )
// Copyright (C) 2004 - 2018 Richard L King (rlking@aultan.com)
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

final class CommandChannel {

    private static final String _Prompt = Settings.settings().getString("CommandPrompt", "");
    private static final boolean _SuppressInfo = Settings.settings().getBoolean("SuppressInfoMessages", true);

    private volatile Socket mSocket;
    private volatile BufferedReader mInstream = null;
    private volatile BufferedWriter mOutstream = null;

    // Set by getCommand() when the incoming line used the "REQ <id> <command>" form,
    // so that writeAck/writeNack can wrap the reply as "RES <id> ...". Cleared when
    // the next line is read in plain form, or at EOF.
    private volatile String mCurrentRequestId = null;

    // True once the connection has issued SUBSCRIBE. Used to reject a second SUBSCRIBE
    // and to drive removeSubscriber() on close().
    private volatile boolean mSubscribed = false;

    // Serialises every write to mOutstream, since the EventBroadcaster thread may push
    // EVENT lines while the dispatcher thread is writing a reply on the same channel.
    private final Object mWriteLock = new Object();

    // Bounded outbound queue for event-stream lines. After SUBSCRIBE is accepted,
    // EventBroadcaster calls startEventStream() to spawn a writer thread that drains
    // this queue. pushEvent then becomes a bounded, non-blocking offer: a slow or
    // stuck subscriber fills the queue and gets dropped at the call site rather than
    // blocking the lifecycle code that fired the event. mWriterThread==null means
    // synchronous writes are still in use for channels that never subscribed.
    private static final int EVENT_QUEUE_LIMIT = 64;
    private final LinkedBlockingQueue<EventLine> mEventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_LIMIT);
    private volatile Thread mWriterThread;

    private static final class EventLine {
        private final String mLine;
        private final CountDownLatch mWritten;
        private final boolean mPreserveBeforeClosing;

        EventLine(String line, CountDownLatch written, boolean preserveBeforeClosing) {
            mLine = line;
            mWritten = written;
            mPreserveBeforeClosing = preserveBeforeClosing;
        }

        void signalWritten() {
            if (mWritten != null) mWritten.countDown();
        }
    }

    CommandChannel(Socket socket) {

        mSocket = socket;
        if (! setupStreams()) return;

        writeInfo("IBC Command Server");
    }

    void close() {
        if (mSubscribed) {
            mSubscribed = false;
            EventBroadcaster.instance().removeSubscriber(this);
        }
        try {
            final Socket socket = mSocket;
            if (socket == null || socket.isClosed()) return;

            Utils.logToConsole("Closing command channel");
            try {
                socket.shutdownInput();
            } catch (IOException e) {
                Utils.logException(e);
            }
            try {
                socket.shutdownOutput();
            } catch (IOException e) {
                Utils.logException(e);
            }

            final BufferedReader instream = mInstream;
            try {
                if (instream != null) instream.close();
            } catch (IOException e) {
                Utils.logException(e);
            }
            mInstream = null;

            final BufferedWriter outstream = mOutstream;
            try {
                if (outstream != null) outstream.close();
            } catch (IOException e) {
                Utils.logException(e);
            }
            mOutstream = null;

            try {
                socket.close();
            } finally {
                if (mSocket == socket) mSocket = null;
            }
        } catch (SocketException e) {
            // the socket was reset by the client - ignore
            Utils.logException(e);
        } catch (IOException e) {
            // ignore
            Utils.logException(e);
        }
    }

    boolean isSubscribed() {
        return mSubscribed;
    }

    void markSubscribed() {
        mSubscribed = true;
    }

    boolean queueSubscribeAccepted(String info, String snapshotState) {
        startEventStream();
        if (!mEventQueue.offer(new EventLine(replyPrefix() + "OK " + info, null, true))) {
            return false;
        }
        if (snapshotState != null && !mEventQueue.offer(new EventLine("STATE " + snapshotState, null, true))) {
            return false;
        }
        return true;
    }

    // Push a single EVENT/STATE line to this channel. Returns false on write failure
    // or, after startEventStream(), on queue-full (slow subscriber). Either way the
    // caller (EventBroadcaster) drops and closes this subscriber.
    boolean pushEvent(String line) {
        if (mOutstream == null) return false;
        if (mWriterThread == null) {
            return synchronousWrite(line);
        }
        return mEventQueue.offer(new EventLine(line, null, false));
    }

    // Queue CLOSING as the final event and wait until the writer has actually
    // returned from synchronousWrite, not merely until it has dequeued the line.
    // The caller closes the socket after this returns; on timeout, that close forces
    // any in-flight blocking write to fail and lets the writer thread exit.
    void writeClosingAndStop(String line, long timeoutMs) {
        if (mOutstream == null) return;
        if (mWriterThread == null) {
            synchronousWrite(line);
            return;
        }
        dropQueuedEventsBeforeClosing();
        final long offerBudgetMs = Math.max(1, timeoutMs / 4);
        final long awaitBudgetMs = Math.max(1, timeoutMs - offerBudgetMs);
        final CountDownLatch written = new CountDownLatch(1);
        final EventLine event = new EventLine(line, written, true);
        try {
            if (!mEventQueue.offer(event, offerBudgetMs, TimeUnit.MILLISECONDS)) return;
            written.await(awaitBudgetMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void dropQueuedEventsBeforeClosing() {
        final List<EventLine> drained = new ArrayList<>();
        mEventQueue.drainTo(drained);
        for (EventLine event : drained) {
            if (event.mPreserveBeforeClosing) mEventQueue.offer(event);
        }
    }

    // Spawn the per-channel writer thread. SUBSCRIBE acceptance queues the ACK
    // and optional STATE snapshot before returning, so subsequent EVENT pushes
    // preserve ordering without blocking lifecycle code.
    void startEventStream() {
        if (mWriterThread != null) return;
        final Thread t = new Thread(this::eventWriterLoop, "IBC-EventWriter");
        t.setDaemon(true);
        mWriterThread = t;
        t.start();
    }

    private boolean synchronousWrite(String line) {
        synchronized (mWriteLock) {
            if (mOutstream == null) return false;
            try {
                mOutstream.write(line);
                mOutstream.newLine();
                mOutstream.flush();
                return true;
            } catch (IOException e) {
                Utils.logException(e);
                return false;
            }
        }
    }

    private void eventWriterLoop() {
        try {
            while (true) {
                final EventLine event = mEventQueue.poll(100, TimeUnit.MILLISECONDS);
                if (mOutstream == null) {
                    if (event != null) event.signalWritten();
                    drainAndSignalRemaining();
                    return;
                }
                if (event == null) continue;
                final boolean written = synchronousWrite(event.mLine);
                event.signalWritten();
                if (!written) {
                    drainAndSignalRemaining();
                    close();
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            drainAndSignalRemaining();
        }
    }

    // Release any latch awaiters so they exit instead of burning their full timeout.
    private void drainAndSignalRemaining() {
        final List<EventLine> drained = new ArrayList<>();
        mEventQueue.drainTo(drained);
        for (EventLine event : drained) {
            event.signalWritten();
        }
    }

    String getCommand() {
        String cmd = null;

        if (mInstream == null) return null;

        try {
            cmd = mInstream.readLine();
            while (cmd != null && cmd.trim().isEmpty()) {
                writePrompt();
                cmd = mInstream.readLine();
            }

            if (cmd != null) Utils.logToConsole("CommandServer received command: " + cmd);
        } catch (SocketException e) {
            // the socket was reset by the client
            Utils.logException(e);
            close();
        } catch (IOException e) {
            Utils.logException(e);
            close();
        }
        if (cmd == null) {
            mCurrentRequestId = null;
            return null;
        }
        return extractCommand(cmd);
    }

    // Detect the "REQ <id> <command>" form. If present, store the id for reply
    // correlation and return just the command portion. Plain form leaves the id
    // null and returns the line unchanged.
    private String extractCommand(String line) {
        final String trimmed = line.trim();
        final int reqEnd = tokenEnd(trimmed, 0);
        if (!trimmed.regionMatches(true, 0, "REQ", 0, 3) || reqEnd != 3)
            return plainCommand(line);
        final int idStart = skipWhitespace(trimmed, reqEnd);
        if (idStart >= trimmed.length())
            return plainCommand(line);
        final int idEnd = tokenEnd(trimmed, idStart);
        final int cmdStart = skipWhitespace(trimmed, idEnd);
        if (cmdStart >= trimmed.length())
            return plainCommand(line);
        mCurrentRequestId = trimmed.substring(idStart, idEnd);
        return trimmed.substring(cmdStart);
    }

    private static int skipWhitespace(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        return i;
    }

    private static int tokenEnd(String s, int i) {
        while (i < s.length() && !Character.isWhitespace(s.charAt(i))) i++;
        return i;
    }

    private String plainCommand(String line) {
        mCurrentRequestId = null;
        return line;
    }

    void writeAck(String info) {
        replyLine(replyPrefix() + "OK " + info);
    }

    // INFO lines are unsolicited progress chatter and are not wrapped in RES,
    // so the request id prefix is intentionally not applied here.
    final void writeInfo(String info) {
        if (! _SuppressInfo) replyLine("INFO " + info);
    }

    void writeNack(String info) {
        replyLine(replyPrefix() + "ERROR " + info);
    }

    private String replyPrefix() {
        return mCurrentRequestId == null ? "" : ("RES " + mCurrentRequestId + " ");
    }

    void writePrompt() {
        if (! _Prompt.isEmpty()) reply(_Prompt);
    }

    private void reply(String message) {
        reply(message, false);
    }

    private void reply(String message, boolean addNewline) {
        synchronized (mWriteLock) {
            if (mOutstream == null) return;
            try {
                mOutstream.write(message);
                if (addNewline) mOutstream.newLine();
                mOutstream.flush();
            } catch (SocketException e) {
                // the socket was reset by the client
                Utils.logException(e);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void replyLine(String message) {
        reply(message,true);
    }

    private boolean setupStreams() {
        try {
            mInstream = new BufferedReader(new InputStreamReader(mSocket.getInputStream()));
            mOutstream = new BufferedWriter(new OutputStreamWriter(mSocket.getOutputStream()));
            return true;
        } catch (IOException e) {
            // this is most likely a result of the user closing the command connection
            Utils.logException(e);
            return false;
        }
    }

}

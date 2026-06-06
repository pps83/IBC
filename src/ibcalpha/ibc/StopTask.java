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

import java.io.File;
import java.util.Arrays;

class StopTask
        implements Runnable {

    private static final SwitchLock _Running = new SwitchLock();
    private static PendingClosing _PendingClosing;

    private static final class PendingClosing {
        final CommandChannel channel;
        final boolean coldRestart;
        final String reason;
        final String closeReason;

        PendingClosing(CommandChannel channel, boolean coldRestart, String reason, String closeReason) {
            this.channel = channel;
            this.coldRestart = coldRestart;
            this.reason = reason;
            this.closeReason = closeReason;
        }
    }

    private final CommandChannel mChannel;
    private final boolean mForceColdRestart;
    private final String mReason;
    private final String mCloseReason;

    StopTask(final CommandChannel channel, final boolean forceColdRestart, final String reason) {
        this(channel, forceColdRestart, reason, forceColdRestart ? "RESTART" : "SHUTDOWN");
    }

    StopTask(final CommandChannel channel, final boolean forceColdRestart, final String reason, final String closeReason) {
        mChannel = channel;
        mForceColdRestart = forceColdRestart;
        mReason = reason;
        mCloseReason = closeReason;
    }

    @Override
    public void run() {
        if (! _Running.set()) {
            Utils.logToConsole("STOP already in progress");
            writeNack("STOP already in progress");
            if (mChannel != null) {
                mChannel.close();
            }
            return;
        }

        try {
            setPendingClosing(mChannel, mForceColdRestart, mReason, mCloseReason);
            writeInfo("Closing IBC");
            stop(mReason);
        } catch (Exception ex) {
            writeNack(ex.getMessage());
            Utils.exitWithException(ErrorCodes.UNHANDLED_EXCEPTION, ex);
        }
    }
    
    private static synchronized void setPendingClosing(CommandChannel channel, boolean forceColdRestart, String reason, String closeReason) {
        _PendingClosing = new PendingClosing(channel, forceColdRestart, reason, closeReason);
    }

    private static synchronized void clearPendingClosing() {
        _PendingClosing = null;
        _Running.clear();
    }

    static boolean commitPendingClosing() {
        final PendingClosing pending;
        synchronized (StopTask.class) {
            if (_PendingClosing == null) {
                return false;
            }
            pending = _PendingClosing;
            _PendingClosing = null;
        }

        String closeReason = pending.closeReason == null ? "SHUTDOWN" : pending.closeReason;
        if (pending.coldRestart) {
            createColdRestartFlagFile();
        }
        EventBroadcaster.instance().beginClosing(closeReason);
        if (pending.channel != null) {
            pending.channel.writeAck("Shutting down: " + pending.reason);
            pending.channel.close();
        }
        return true;
    }

    private static void createColdRestartFlagFile() {
        try {
        new File(System.getProperty("jtsConfigDir") + 
                 File.separator + 
                 "COLDRESTART" + 
                 System.getProperty("ibcsessionid"))
                .createNewFile();
        } catch (java.io.IOException e) {
            Utils.exitWithException(ErrorCodes.UNHANDLED_EXCEPTION, e);
        }
    }

    public final static boolean shutdownInProgress()
    {
        return _Running.query();
    }

    private void stop(String reason) {
        try {
            if (LoginManager.loginManager().getLoginState() != LoginManager.LoginState.LOGGED_IN) {
                Utils.logToConsole("Login has not completed: exiting immediately");
                commitPendingClosing();
                Runtime.getRuntime().halt(0);
            } else {
                String[] closeMenuPath = SessionManager.isGateway() ? new String[] {"File", "Close"} : new String[] {"File", "Exit"};
                Utils.logToConsole("Login has completed: exiting via " + Arrays.deepToString(closeMenuPath) + " menu");
                if (!Utils.invokeMenuItem(MainWindowManager.mainWindowManager().getMainWindow(), closeMenuPath)) {
                    writeNack("could not invoke " + Arrays.deepToString(closeMenuPath));
                    if (mChannel != null) {
                        mChannel.close();
                    }
                    clearPendingClosing();
                }
            }
            
        } catch (Exception e) {
            writeNack(e.getMessage());
            if (mChannel != null) {
                mChannel.close();
            }
            clearPendingClosing();
        }
    }

    private void writeAck(String message) {if (mChannel != null) mChannel.writeAck(message);}
    private void writeInfo(String message) {if (mChannel != null) mChannel.writeInfo(message);}
    private void writeNack(String message) {if (mChannel != null) mChannel.writeNack(message);}

}

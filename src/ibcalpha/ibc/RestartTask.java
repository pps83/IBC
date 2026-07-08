// This file is part of IBC.
// Copyright (C) 2004 Steven M. Kearns (skearns23@yahoo.com )
// Copyright (C) 2004 - 2023 Richard L King (rlking@aultan.com)
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

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import static java.awt.GraphicsDevice.WindowTranslucency.TRANSLUCENT;
import java.awt.GraphicsEnvironment;
import java.awt.IllegalComponentStateException;
import java.io.File;
import static java.lang.Thread.sleep;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import javax.swing.JFrame;
import javax.swing.JPanel;

class RestartTask
        implements Runnable {

    private static final SwitchLock _Running = new SwitchLock();
    private static PendingTerminalClose _PendingTerminalClose;
    private static boolean _GatewayPauseScheduled;

    private static final class PendingTerminalClose {
        final CommandChannel channel;
        final String closeReason;
        final String ackMessage;
        final String requestId; // captured at command time; the channel's live id may be a later command's

        PendingTerminalClose(CommandChannel channel, String closeReason, String ackMessage, String requestId) {
            this.channel = channel;
            this.closeReason = closeReason;
            this.ackMessage = ackMessage;
            this.requestId = requestId;
        }
    }

    private final CommandChannel mChannel;
    
    private final boolean mPauseOnly;

    private final String mVerb;

    RestartTask(final CommandChannel channel,
                final boolean pauseOnly) {
        mChannel = channel;
        mPauseOnly = pauseOnly;
        mVerb = mPauseOnly ? "PAUSE" : "RESTART";
    }

    @Override
    public void run() {
        if (! _Running.set()) {
            Utils.logToConsole(mVerb + " already in progress");
            writeNack(mVerb + " already in progress");
            if (mChannel != null) {
                mChannel.close();
            }
            return;
        }

        try {
            if (mPauseOnly) {
                writeInfo("Pausing TWS");
                createPauseFlagFile();
            } else {
                writeInfo("Restarting TWS");
                EventBroadcaster.instance().emitRestarting();
            }
            restart(mPauseOnly);
        } catch (Exception ex) {
            writeNack(ex.getMessage());
            Utils.exitWithException(ErrorCodes.UNHANDLED_EXCEPTION, ex);
        }
    }
    
    private void createPauseFlagFile() {
        try {
        new File(System.getProperty("jtsConfigDir") + 
                 File.separator + 
                 "PAUSE" + 
                 System.getProperty("ibcsessionid"))
                .createNewFile();
        } catch (java.io.IOException e) {
            Utils.exitWithException(ErrorCodes.UNHANDLED_EXCEPTION, e);
        }
    }

    void restart(final boolean pauseOnly) {
        setPendingTerminalClose(mChannel, pauseOnly ? "SHUTDOWN" : "RESTART", mVerb + " in progress");
        if (Utils.invokeMenuItem(MainWindowManager.mainWindowManager().getMainWindow(), new String[] {"File", "Restart..."})) {
            return;
        }
        clearPendingTerminalClose(false);

        while (LocalTime.now().getSecond() >= 58) {
            try {sleep(1);} catch (InterruptedException e) {}
        }

        LocalTime now = LocalTime.now();

        int newHour = now.getHour();
        int newMinute = now.getMinute() + 1;
        if (newMinute > 59) {
            newMinute = newMinute - 60;
            newHour = newHour + 1;
        }
        if (newHour > 23) {
            newHour = newHour - 24;
        }
        LocalTime actionTime = now.withHour(newHour).withMinute(newMinute).withSecond(0);

        Utils.logToConsole("Setting auto-restart time to " + actionTime.format(DateTimeFormatter.ofPattern("hh:mm a")));
        ConfigurationTask task = new ConfigurationTask(new ConfigureAutoLogoffOrRestartTimeTask(
                                        "Auto restart",
                                        actionTime)
                                );
        boolean configured = task.execute();
        if (!configured) {
            String why = task.lastErrorMessage();
            writeNack("could not set auto-restart time" + (why.isEmpty() ? "" : ": " + why));
            if (mChannel != null) {
                mChannel.close();
            }
            clearPendingTerminalClose(true);
            return;
        }

        setGatewayPauseScheduled(pauseOnly);
        writeAck(mVerb + " at "  + actionTime.format(DateTimeFormatter.ofPattern("hh:mm a")));
        if (mChannel != null) {
            mChannel.close();
        }
        // Keep _Running set until the scheduled shutdown fires; another
        // RESTART/PAUSE before then would race the same Gateway setting.

        try {
            if (!GraphicsEnvironment.getLocalGraphicsEnvironment().getDefaultScreenDevice().isWindowTranslucencySupported(TRANSLUCENT)) return;
        } catch (IllegalComponentStateException e) {
            return;
        }

        JFrame window = MainWindowManager.mainWindowManager().getMainWindow();
        window.setOpacity(0.80f);
        
        RestartTask.Countdown countdown = new RestartTask.Countdown(actionTime.isBefore(LocalTime.now()) 
                                            ? actionTime.atDate(LocalDate.now().plusDays(1)) 
                                            : actionTime.atDate(LocalDate.now()));
        window.setGlassPane(countdown);
        countdown.setVisible(true);
    }

    private static synchronized void setPendingTerminalClose(CommandChannel channel, String closeReason, String ackMessage) {
        _PendingTerminalClose = new PendingTerminalClose(channel, closeReason, ackMessage,
                channel == null ? null : channel.currentRequestId());
    }

    private static synchronized void clearPendingTerminalClose(boolean releaseRunning) {
        _PendingTerminalClose = null;
        if (releaseRunning) {
            _Running.clear();
        }
    }

    static boolean commitPendingTerminalClose() {
        final PendingTerminalClose pending;
        synchronized (RestartTask.class) {
            if (_PendingTerminalClose == null) {
                return false;
            }
            pending = _PendingTerminalClose;
            _PendingTerminalClose = null;
        }

        EventBroadcaster.instance().beginClosing(pending.closeReason);
        if (pending.channel != null) {
            pending.channel.writeAck(pending.requestId, pending.ackMessage);
            pending.channel.close();
        }
        return true;
    }

    private static synchronized void setGatewayPauseScheduled(boolean scheduled) {
        _GatewayPauseScheduled = scheduled;
    }

    static synchronized boolean consumeGatewayPauseScheduled() {
        boolean result = _GatewayPauseScheduled;
        _GatewayPauseScheduled = false;
        return result;
    }

    private void writeAck(String message) {if (mChannel != null) mChannel.writeAck(message);}
    private void writeInfo(String message) {if (mChannel != null) mChannel.writeInfo(message);}
    private void writeNack(String message) {if (mChannel != null) mChannel.writeNack(message);}

    private class Countdown extends JPanel {
        private  static final long serialVersionUID = 1L;
        
        private final Font font = new Font("Arial", Font.BOLD, 36);
        private final LocalDateTime countdownTo;
        private Duration secsRemaining;
        
        public Countdown(final LocalDateTime countdownTo) {
            this.setSize(250, 250);
            this.setOpaque(false);
            this.countdownTo = countdownTo;
            MyScheduledExecutorService.getInstance().scheduleAtFixedRate(  () -> {
                                                secsRemaining = Duration.between(LocalDateTime.now(), this.countdownTo);
                                                this.repaint();
                                            }, 
                                            0, 
                                            1, 
                                            TimeUnit.SECONDS);
        }

        @Override
        public void paintComponent(Graphics g) {
            super.paintComponent(g);       
            
            g.setColor(Color.WHITE);
            g.setFont(font);
            if (secsRemaining.isNegative()) {
                g.drawString(mVerb + " in progress", 50, 300);
            } else {
                g.drawString(mVerb + " in " + secsRemaining.getSeconds() + " seconds", 50, 300);
            }
        }  
    }
}

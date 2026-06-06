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

import java.util.concurrent.FutureTask;
import javax.swing.JDialog;

public class ConfigurationTask {

    private final ConfigurationAction configAction;
    private volatile Exception lastException;

    public ConfigurationTask(ConfigurationAction configAction) {
        this.configAction = configAction;
    }

    public String lastErrorMessage() {
        Throwable t = lastException;
        String message = "";
        String fallback = "";
        while (t != null) {
            final String current = t.getMessage();
            if (current != null && !current.isEmpty()) message = current;
            if (fallback.isEmpty()) fallback = t.getClass().getSimpleName();
            t = t.getCause();
        }
        return message.isEmpty() ? fallback : message;
    }

    public void executeAsync() {
        MyCachedThreadPool.getInstance().execute(new ConfigTaskRunner());
    }

    public boolean execute() {
        return (new ConfigTaskRunner()).runTask();
    }

    private class ConfigTaskRunner implements Runnable {
        @Override
        public void run() {
            runTask();
        }

        boolean runTask() {
            lastException = null;
            JDialog configDialog = null;
            try {
                configDialog = ConfigDialogManager.configDialogManager().getConfigDialog();    // blocks the thread until the config dialog is available
                configAction.initialise(configDialog);
   
                FutureTask<?> t = new FutureTask<>((Runnable)configAction, null);
                GuiExecutor.instance().execute(t);
                t.get();
                return true;
            } catch (Exception e){
                lastException = e;
                Utils.logException(e);
                return false;
            } finally {
                if (configDialog != null) ConfigDialogManager.configDialogManager().releaseConfigDialog();
            }
        }
    }


}

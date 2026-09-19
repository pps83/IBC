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

/**
 * The contract with a launcher that starts this JVM directly instead of through the scripts.
 *
 * The launcher validates and selects every value here before spawning, so IBC only consumes them:
 * no defaulting, no reinterpretation, no logging. With the mode variable absent IBC behaves exactly
 * as it always has, which is what keeps the stock scripts working.
 */
final class NativeLaunch {

    private static final String MODE_VARIABLE = "IBC_LAUNCHER_MODE";
    private static final String NATIVE_MODE = "native";
    private static final String USER_ID_VARIABLE = "IBC_USER_ID";
    private static final String PASSWORD_VARIABLE = "IBC_PASSWORD";

    /** Parsed by the launcher out of stdout, so this is protocol text and not a log message. */
    private static final String AUTHENTICATION_REJECTED = "IBC_NATIVE_EVENT AUTHENTICATION_REJECTED";
    /** Followed by a reason token and the gateway's own words for the refusal, on that one line. */
    private static final String LOGIN_REFUSED_PREFIX = "IBC_NATIVE_EVENT LOGIN_REFUSED ";
    static final String REASON_BAD_CREDENTIALS = "BAD_CREDENTIALS";
    static final String REASON_WRONG_MODE = "WRONG_MODE";
    static final String REASON_UNAVAILABLE = "UNAVAILABLE";
    static final String REASON_OTHER_SESSION = "OTHER_SESSION";
    private static final String REASON_LOCKOUT = "LOCKOUT";

    private NativeLaunch() { }

    static boolean isNativeMode() {
        return NATIVE_MODE.equals(System.getenv(MODE_VARIABLE));
    }

    static String userId() {
        return System.getenv(USER_ID_VARIABLE);
    }

    static String password() {
        return System.getenv(PASSWORD_VARIABLE);
    }

    /** Reports that IBKR definitively rejected the credentials. Terminating is the launcher's decision. */
    static void reportAuthenticationRejected() {
        Utils.logRawToConsole(AUTHENTICATION_REJECTED);
    }

    /**
     * Reports a login-phase dialog in which the gateway refused to proceed: the reason the handler recognised,
     * then the dialog's text on the same line. Terminating is the launcher's decision; the dialog stays open.
     */
    static void reportLoginRefused(String reason, String dialogText) {
        String oneLine = dialogText == null ? "" : dialogText.replaceAll("<[^>]*>", " ").replaceAll("\\s+", " ").trim();
        Utils.logRawToConsole(LOGIN_REFUSED_PREFIX + reason + " " + oneLine);
    }

    /** The lockout reason carries the seconds IBKR asked for, as the handler parsed them. */
    static String lockoutReason(long seconds) {
        return REASON_LOCKOUT + " " + seconds;
    }

}

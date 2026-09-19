package org.minicache.common;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class AuthManager {
    private final String expectedToken;
    private final boolean authEnabled;

    public AuthManager(String username, String password) {
        if (username != null && !username.isBlank() && password != null && !password.isBlank()) {
            String raw = username + ":" + password;
            this.expectedToken = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
            this.authEnabled = true;
        } else {
            this.expectedToken = null;
            this.authEnabled = false;
        }
    }

    public boolean isAuthDisabled() {
        return !authEnabled;
    }

    public boolean authenticate(String authToken) {
        if (!authEnabled) return true;
        if (authToken == null || authToken.isBlank()) return false;
        return expectedToken.equals(authToken);
    }
}

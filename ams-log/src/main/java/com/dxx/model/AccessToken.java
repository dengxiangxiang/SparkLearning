package com.dxx.model;

import org.apache.commons.lang.StringUtils;

public class AccessToken {
    private final String version;
    private final String tokenId;
    private final String data;
    private final long accessTokenExpiryTime;
    private final String checkSum;
    private static final String POINTER = ".";
    private static final String SPLIT_POINTER = "\\.";

    public AccessToken(final String version, final String tokenId, final String data, final long accessTokenExpiryTime, final String checkSum) {
        this.version = version;
        this.tokenId = tokenId;
        this.data = data;
        this.accessTokenExpiryTime = accessTokenExpiryTime;
        this.checkSum = checkSum;
    }

    public String getVersion() {
        return this.version;
    }

    public String getTokenId() {
        return this.tokenId;
    }

    public String getData() {
        return this.data;
    }

    public long getAccessTokenExpiryTime() {
        return this.accessTokenExpiryTime;
    }

    public String getCheckSum() {
        return this.checkSum;
    }

    public String encode() {
        final String tokenString = this.version + POINTER + this.tokenId + POINTER + this.data + POINTER + this.accessTokenExpiryTime;
        final String secureToken = tokenString + POINTER + this.checkSum;

        return secureToken;
    }

    public static AccessToken parse(final String accessTokenStr) {
        try {
            if ((accessTokenStr != null) && (accessTokenStr.contains(POINTER))) {
                final String[] tokens = accessTokenStr.split(SPLIT_POINTER);

                if ((tokens != null) && (tokens.length == 5)) {
                    final String version = tokens[0];
                    final String tokenId = encode(tokens[1]);
                    final String data = tokens[2];
                    final long expiryTime = Long.parseLong(tokens[3]);
                    final String checkSum = tokens[4];
                    return new AccessToken(version, tokenId, data, expiryTime, checkSum);
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        throw new IllegalArgumentException("Invalid access token: " + accessTokenStr);
    }

    public static String fetchUserName(final String accessTokenStr) {
        try {
            if ((accessTokenStr != null) && (accessTokenStr.contains(POINTER))) {
                final String[] tokens = accessTokenStr.split(SPLIT_POINTER);

                if ((tokens != null) && (tokens.length == 5)) {
                    final String version = tokens[0];
                    final String tokenId = tokens[1];
                    final String data = tokens[2];
                    final long expiryTime = Long.parseLong(tokens[3]);
                    final String checkSum = tokens[4];



                    String[] split =null;
                    if(tokenId.contains("/")){
                        split = tokenId.split("/");
                    }else{
                        split = tokenId.split("%2F");
                    }

                    String user = encode( split[1]);
                    return user;

                }
            }
        } catch (final Exception e) {
            throw e;
        }

        throw new IllegalArgumentException("Invalid access token: " + accessTokenStr);
    }

    private static String encode(String str) {
        if (StringUtils.isEmpty(str)) {
            return str;
        } else {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < str.length(); ++i) {
                char c = str.charAt(i);
                if (c >= 'a' && c <= 'm') {
                    c = (char) (c + 13);
                } else if (c >= 'A' && c <= 'M') {
                    c = (char) (c + 13);
                } else if (c >= 'n' && c <= 'z') {
                    c = (char) (c - 13);
                } else if (c >= 'N' && c <= 'Z') {
                    c = (char) (c - 13);
                } else if (c >= '0' && c <= '4') {
                    c = (char) (c + 5);
                } else if (c >= '5' && c <= '9') {
                    c = (char) (c - 5);
                }

                sb.append(c);
            }

            return sb.toString();
        }
    }
}
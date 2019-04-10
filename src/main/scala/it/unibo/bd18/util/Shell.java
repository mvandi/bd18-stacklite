package it.unibo.bd18.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Shell {

    private static final Runtime INSTANCE = Runtime.getRuntime();

    public static Environment environment() {
        return new Environment();
    }

    public static final class Environment {
        private final Map<String, String> variables = new LinkedHashMap<>();

        private Environment() {
        }

        public Environment set(String name, boolean value) {
            return set(name, value ? "1" : "0");
        }

        public Environment set(String name, byte value) {
            return set(name, Byte.toString(value));
        }

        public Environment set(String name, short value) {
            return set(name, Short.toString(value));
        }

        public Environment set(String name, int value) {
            return set(name, Integer.toString(value));
        }

        public Environment set(String name, long value) {
            return set(name, Long.toString(value));
        }

        public Environment set(String name, float value) {
            return set(name, Float.toString(value));
        }

        public Environment set(String name, double value) {
            return set(name, Double.toString(value));
        }

        public Environment set(String name, String value) {
            variables.put(name, value);
            return this;
        }

        public Environment unset(String name) {
            variables.remove(name);
            return this;
        }

        public String[] build() {
            if (variables.isEmpty()) return null;
            final String[] result = toArray();
            variables.clear();
            return result;
        }

        private String[] toArray() {
            final String[] result = new String[variables.size()];
            int i = 0;
            for (Entry<String, String> e : variables.entrySet())
                result[i++] = e.getKey() + "=" + quote(e.getValue());
            return result;
        }
    }

    public static List<String> exec(String command) {
        return exec(command, null, (File) null);
    }

    public static List<String> exec(String command, Environment env) {
        return exec(command, env, (File) null);
    }

    public static List<String> exec(String command, String dir) {
        return exec(command, null, new File(dir));
    }

    public static List<String> exec(String command, Environment env, String dir) {
        return exec(command, env, new File(dir));
    }

    public static List<String> exec(String command, Environment env, File dir) {
        final String resolvedCommand = resolveEnvironmentVariables(command);
        return exec0(isWindows() ? "cmd.exe /c " + resolvedCommand : "sh -c " + resolvedCommand,
                env == null ? null : env.build(),
                dir);
    }

    private static List<String> exec0(String command, String[] envp, File dir) {
        try {
            final Process p = INSTANCE.exec(command, envp, dir);
            try (final BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                final List<String> lines = new ArrayList<>();
                String line;
                while ((line = in.readLine()) != null) {
                    lines.add(line);
                }
                return lines;
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String resolveEnvironmentVariables(String command) {
        final Pattern variablePattern = Pattern.compile(isWindows()
                ? "%(\\w+?)%"
                : "[$](\\{(\\w+?)\\}|\\w+?)");

        final Matcher m = variablePattern.matcher(command);

        final StringBuilder sb = new StringBuilder(command.length());
        int lastMatchEnd = 0;
        while (m.find()) {
            sb.append(command, lastMatchEnd, m.start());
            final String variable = m.group(1).replaceAll("(\\W)+", "");
            final String value = System.getenv(variable);
            if (value == null)
                sb.append(command.substring(m.start(), m.end()));
            else
                sb.append(quote(value));
            lastMatchEnd = m.end();
        }
        sb.append(command.substring(lastMatchEnd));

        return sb.toString();
    }

    private static String quote(String s) {
        return s.contains(" ")
                ? "\"" + s + "\""
                : s;
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }

    private Shell() {
    }

}

package io.millesabords.demo.streamingsql.cli;

import io.millesabords.demo.streamingsql.flink.LogStreamSqlProcessor;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;

public class SqlCli {

    private static Thread currentWorker;
    private static Thread waitUserAction;

    public static void main(final String[] args) throws IOException {

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (currentWorker != null) {
                    currentWorker.stop();
                }
            }
        });

        final Terminal terminal = TerminalBuilder.terminal();
        final LineReader lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .parser(new SqlParser())
                .build();
        lineReader.setVariable(LineReader.HISTORY_FILE, "streaming-sql.history");

        /*final Console console = System.console();
        if (console == null) {
            System.out.println("No console: non-interactive mode!");
            System.exit(0);
        }*/

        final StringBuilder buffer = new StringBuilder();
        final String prompt = "sql > ";

        while (true) {
            //System.out.print(prompt);
            //final String query = console.readLine();
            final String query = lineReader.readLine(prompt);

            buffer.append(query.trim() + " ");

            if (query.trim().endsWith(";")) {
                String fullQuery = buffer.toString().trim().toLowerCase();
                fullQuery = fullQuery.substring(0, fullQuery.length() - 1); // Remove ;
                fullQuery = fullQuery.replaceAll("stream", "");
                buffer.delete(0, buffer.length());
                processRequest(fullQuery);

                waitUserStop();
            }
        }
    }

    private static void waitUserStop() {
        waitUserAction = new Thread(() -> {
            boolean stopWorker = false;

            while (!stopWorker) {
                final char[] read = System.console().readPassword();
                stopWorker = read.length > 0 && read[0] == 's';
            }

            if (currentWorker != null) {
                currentWorker.stop();
            }
        });
        waitUserAction.start();

        try {
            waitUserAction.join();
        }
        catch (final InterruptedException e) {
        }
    }

    private static void processRequest(final String query) {
        currentWorker = new Thread(() -> {
            try {
                new LogStreamSqlProcessor().run(query);
            }
            catch (final Exception e) {
                if (!(e instanceof InterruptedException)) {
                    System.out.println("ERROR: " + e.getMessage());
                }
                waitUserAction.stop();
            }
        });
        currentWorker.start();
    }

    static class SqlParser extends DefaultParser {

        @Override
        public ParsedLine parse(final String s, final int i, final ParseContext parseContext) throws SyntaxError {
            if (!s.trim().endsWith(";")) throw new EOFError(i, 0, "", "... ");
            final ParsedLine line = super.parse(s, i, parseContext);
            return line;
        }
    }
}

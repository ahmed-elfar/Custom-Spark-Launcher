package com.xy.spark.launcher;

import java.util.Observable;
import java.util.Observer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class StreamListener implements Observer {

    public static class Builder {
        private Consumer<String> lineProcessor;
        private Pattern lineMatcher;
        private int maxLinesCount;

        public Builder (Consumer<String> lineProcessor) {
            this.lineProcessor = lineProcessor;
        }

        public Builder withFilter(Pattern filter) {
            this.lineMatcher = filter;
            return this;
        }

        public Builder withLoggingFilter() {
            // https://stackoverflow.com/questions/43121432/regular-expression-log-parsing
            // https://regex101.com/r/LJnVrS/1
            this.lineMatcher = ONLY_LOG4J;
            return this;
        }

        public Builder withNoLoggingFilter() {
            //FIXME double check the negation
            this.lineMatcher = Pattern.compile("(!?(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) (?<level>INFO|ERROR|WARN|TRACE|DEBUG|FATAL)\\s+\\[(?<class>[^\\]]+)]-\\[(?<thread>[^\\]]+)]\\s+(?<text>.*?)(?=\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}|\\Z)\n)", Pattern.CASE_INSENSITIVE);
            return this;
        }

        public Builder withErrorsOnlyFilter() {
            this.lineMatcher = ONLY_ERRORS;
            return this;
        }

        public Builder withMaxLoggedLines(int maxLinesCount) {
            this.maxLinesCount = maxLinesCount;
            return this;
        }

        public StreamListener build() {
            StreamListener streamListener;
            if (maxLinesCount > 0) {
                streamListener = new BoundedStreamListener(lineProcessor, maxLinesCount);
            } else {
                streamListener = new StreamListener();
                streamListener.lineProcessor = lineProcessor;
            }
            streamListener.lineMatcher = lineMatcher;
            return streamListener;
        }
    }

    static class BoundedStreamListener extends StreamListener {
        private final int linesCount;
        private int loggedLines;
        private BoundedStreamListener(Consumer<String> processor, int linesCount){
            super.lineProcessor = s -> { if (loggedLines++ < linesCount) processor.accept(s); };
            this.linesCount = linesCount;
        }
    }

    private final static Pattern ONLY_LOG4J = Pattern.compile("(?<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) (?<level>INFO|ERROR|WARN|TRACE|DEBUG|FATAL)\\s+\\[(?<class>[^\\]]+)]-\\[(?<thread>[^\\]]+)]\\s+(?<text>.*?)(?=\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}|\\Z)\n", Pattern.CASE_INSENSITIVE);
    private final static Pattern ONLY_ERRORS = Pattern.compile("(\\berror\\b|\\bexception\\b|\\bcaused by\\b)", Pattern.CASE_INSENSITIVE);
    private Consumer<String> lineProcessor;
    private Pattern lineMatcher;

    private StreamListener() {}

    @Override
    public void update(Observable observable, Object data) {
        if (data != null) {
            String line = String.valueOf(data);
            if (lineMatcher==null || lineMatcher.matcher(line).find()) lineProcessor.accept(line);
        }
    }
}
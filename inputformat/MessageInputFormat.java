package com.sebastianpedersen.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class MessageInputFormat extends FileInputFormat<Text, Text> {
    private static final Log LOG = LogFactory.getLog(MessageInputFormat.class.getName());

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new MessageRecordReader((FileSplit) split, job);
    }

    public static class MessageRecordReader implements RecordReader<Text, Text> {
        private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;

        private final FSDataInputStream stream;
        private final MessageReader reader;

        private final long start;
        private final long end;
        private final long skip;

        private long position;

        public MessageRecordReader(FileSplit split, JobConf configuration) throws IOException {
            start = split.getStart();
            end = start + split.getLength();

            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(configuration);

            stream = fs.open(file);
            stream.seek(start);

            reader = new MessageReader(stream, configuration.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));

            skip = reader.skipMessage();
            position = start + skip;
        }

        @Override
        public boolean next(Text key, Text value) throws IOException {
            if (position < end && reader.moreMessages()) {
                position += reader.nextMessage(key, value);

                return true;
            }

            return false;
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (position - start) / (float) (end - start));
            }
        }
    }
}
package com.sebastianpedersen.hadoop.mapred;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class MessageReader implements Closeable {
    private static final Log LOG = LogFactory.getLog(MessageReader.class.getName());

    private static final byte[] MESSAGE_IDENTIFIER = "\",\"Message-ID: ".getBytes(Charsets.UTF_8);
    private static final byte NEWLINE = '\n';
    private static final byte RECORD_SEPARATOR = '\30';

    private final InputStream stream;

    private final byte[] buffer;
    private final int bufferSize;

    // Bytes currently read into the buffer.
    private int bytesInBuffer = 0;
    // Current position in the buffer.
    private int bufferPosition = 0;

    // Indicates whether we have reached EOF or not.
    private boolean done = false;

    public MessageReader(InputStream stream, int bufferSize) {
        this.stream = stream;
        this.bufferSize = bufferSize;
        this.buffer = new byte[bufferSize];
    }

    /**
     * Closes the underlying input stream.
     */
    @Override
    public void close() throws IOException {
        stream.close();
    }

    /**
     * Checks whether we can read more messages.
     * <p>
     * <b>Note</b> that it's the caller responsibility to first check that we have not read past the end of the split.
     */
    public boolean moreMessages() {
        return !done;
    }

    /**
     * Tries to skip a message.
     * Either a message is skipped or <b>EOF</b> is reached.
     * IF <b>EOF</b> is reached {@link #moreMessages()} is set to return false.
     */
    public int skipMessage() throws IOException {
        return readMessage(null, null, false);
    }

    /**
     * Reads the next message-id and message into the objects provided.
     */
    public int nextMessage(Text key, Text value) throws IOException {
        return readMessage(key, value, true);
    }

    private int readMessage(Text key, Text value, boolean store) throws IOException {
        // It's really important to clear these as objects are reused.
        if (store) {
            key.clear();
            value.clear();
        }

        int bytesRead = 0;
        int identifierMatch = 0;
        int restore = 0;

        boolean processKey = true;

        // Keep reading until we reach the start of a new message.
        while (identifierMatch < MESSAGE_IDENTIFIER.length) {
            int start = bufferPosition;

            // Buffer is full.
            if (bufferPosition >= bytesInBuffer) {
                // Reset position in the buffer.
                start = bufferPosition = 0;

                // Try to read some bytes into the buffer.
                bytesInBuffer = stream.read(buffer);

                // We've reached EOF.
                if (bytesInBuffer <= 0) {
                    if (restore > 0) {
                        value.append(MESSAGE_IDENTIFIER, 0, restore);

                        bytesRead += restore;
                    }

                    // Number of bytes read since EOF.
                    int appendLength = bufferPosition - start;

                    if (store && appendLength > 0) {
                        value.append(buffer, start, appendLength);
                    }

                    bytesRead++;
                    done = true;

                    return bytesRead;
                }
            }

            // Iterate over bytes in buffer.
            while (bufferPosition < bytesInBuffer) {
                bytesRead++;

                byte nextChar = buffer[bufferPosition];

                if (nextChar == NEWLINE) {
                    // If we still need to process the key.
                    if (store && processKey) {
                        int appendLength = bufferPosition - start;

                        key.append(buffer, start, appendLength);
                        // Skip key length + newline.
                        start += appendLength + 1;

                        processKey = false;
                    }

                    // We replace all newlines with the ascii record separator.
                    buffer[bufferPosition] = RECORD_SEPARATOR;
                }

                // Check if character is part of the message identifier.
                if (nextChar == MESSAGE_IDENTIFIER[identifierMatch]) {
                    identifierMatch++;

                    // We have matched the whole identifier.
                    if (identifierMatch >= MESSAGE_IDENTIFIER.length) {
                        bufferPosition++;

                        // Break from the inner loop.
                        // Note that the outer loop condition is no longer satisfied.
                        break;
                    }
                } else if (identifierMatch != 0) {
                    // Character was not part of the identifier in position n.
                    // We need to retry the same character in position 0.
                    bufferPosition--;
                    bytesRead--;
                    identifierMatch = 0;
                }

                bufferPosition++;
            }

            // Move current buffer into value but don't include the identifier.
            int readLength = bufferPosition - start;
            int appendLength = readLength - identifierMatch;

            if (store && appendLength > 0) {
                // Restore part assumed to be identifier.
                // Note that if it were in fact the full identifier appendLength would be 0
                // and we would not be here.
                if (restore > 0) {
                    value.append(MESSAGE_IDENTIFIER, 0, restore);

                    bytesRead += restore;
                    restore = 0;
                }

                value.append(buffer, start, appendLength);
            }

            // If we read past the buffer we might need to restore part of the identifier.
            if (bufferPosition >= bytesInBuffer) {
                if (identifierMatch > 0 && identifierMatch < MESSAGE_IDENTIFIER.length) {
                    restore = identifierMatch;
                    bytesRead -= restore;
                }
            }
        }

        return bytesRead;
    }
}

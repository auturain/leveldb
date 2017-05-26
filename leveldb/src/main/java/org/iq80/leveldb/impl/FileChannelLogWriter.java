/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import org.iq80.leveldb.util.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;

public class FileChannelLogWriter
        implements LogWriter
{
    private final File file;
    private final long fileNumber;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Current offset in the current block
     */
    private int blockOffset;

    public FileChannelLogWriter(File file, long fileNumber)
            throws FileNotFoundException
    {
        Preconditions.checkNotNull(file, "file is null");
        Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");

        this.file = file;
        this.fileNumber = fileNumber;
        this.fileChannel = new FileOutputStream(file).getChannel();
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public synchronized void close()
    {
        closed.set(true);

        // try to forces the log to disk
        try {
            fileChannel.force(true);
        }
        catch (IOException ignored) {
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
    }

    @Override
    public synchronized void delete()
    {
        closed.set(true);

        // close the channel
        Closeables.closeQuietly(fileChannel);

        // try to delete the file
        file.delete();
    }

    @Override
    public File getFile()
    {
        return file;
    }

    @Override
    public long getFileNumber()
    {
        return fileNumber;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    @Override
    public synchronized void addRecord(Slice record, boolean force)
            throws IOException
    {
        Preconditions.checkState(!closed.get(), "Log has been closed");

        SliceInput sliceInput = record.input();

        // used to track first, middle and last blocks
        boolean begin = true;

        // Fragment the record int chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.
        //分割写入的记录为指定的大小，然后不足的就填充0？
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset; //BLOCK_SIZE是固定的32768，剩下的块数，难道这个blockOffset不会报null？
            Preconditions.checkState(bytesRemainingInBlock >= 0); //检验剩余的块数

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < HEADER_SIZE) { // Header is checksum (4 bytes), type (1 byte), length (2 bytes).
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    fileChannel.write(ByteBuffer.allocate(bytesRemainingInBlock));
                }
                blockOffset = 0; //换新的block了？
                bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            }

            // Invariant: we never leave less than HEADER_SIZE bytes available in a block
            int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE; //一个block的剩余空间
            Preconditions.checkState(bytesAvailableInBlock >= 0);

            // if there are more bytes in the record then there are available in the block,
            // fragment the record; otherwise write to the end of the record
            boolean end;
            int fragmentLength;
            if (sliceInput.available() > bytesAvailableInBlock) { //要写入的数据大于这个block的剩余空间
                end = false; //还没结束
                fragmentLength = bytesAvailableInBlock; //分块的大小就是剩余空间的大小
            } else { //如果要写入的数据小于这个block的剩余空间了，装不满
                end = true; //证明要结束了
                fragmentLength = sliceInput.available();
            }

            // determine block type
            LogChunkType type;
            if (begin && end) {
                type = LogChunkType.FULL; //直接一条写完
            }
            else if (begin) {
                type = LogChunkType.FIRST; //第一次写
            }
            else if (end) {
                type = LogChunkType.LAST;  //最后写
            }
            else {
                type = LogChunkType.MIDDLE;  //在写的过程中
            }

            // write the chunk
            writeChunk(type, sliceInput.readSlice(fragmentLength)); //那就写吧，用readSlice把写的内容按照fragmentLength分块

            // we are no longer on the first chunk
            begin = false;  //这是已经不是第一次写了
        } while (sliceInput.isReadable()); //看slice还有没有可读的

        if (force) { //是否强制数据从缓存写入到磁盘
            fileChannel.force(false);
        }
    }

    private void writeChunk(LogChunkType type, Slice slice)
            throws IOException
    {
        Preconditions.checkArgument(slice.length() <= 0xffff, "length %s is larger than two bytes", slice.length());
        Preconditions.checkArgument(blockOffset + HEADER_SIZE <= BLOCK_SIZE);

        // create header
        Slice header = newLogRecordHeader(type, slice, slice.length()); // Header is checksum 校验，为 slice 对象计算出(4 bytes), type (1 byte), length (2 bytes).

        // write the header and the payload
        header.getBytes(0, fileChannel, header.length()); //这就写了？
        slice.getBytes(0, fileChannel, slice.length());

        blockOffset += HEADER_SIZE + slice.length(); //写了这么长
    }

    private Slice newLogRecordHeader(LogChunkType type, Slice slice, int length)
    {
        int crc = Logs.getChunkChecksum(type.getPersistentId(), slice.getRawArray(), slice.getRawOffset(), length);

        // Format the header
        //按位存储
        SliceOutput header = Slices.allocate(HEADER_SIZE).output();
        header.writeInt(crc);
        header.writeByte((byte) (length & 0xff));
        header.writeByte((byte) (length >>> 8));
        header.writeByte((byte) (type.getPersistentId()));

        return header.slice();
    }
}

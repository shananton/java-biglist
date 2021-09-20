package com.github.shananton.biglist;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractList;

public class GrowingBigList extends AbstractList<Long> {
    private static final long MIN_IN_RAM_LIMIT = 1;

    private final int inRamLimit;
    private final FileChannel fileChannel;
    private final ByteBuffer segmentBuffer;
    private final LongBuffer longViewBuffer;
    private int size = 0;
    private int activeSegment = 0;

    public GrowingBigList(long inRamLimit) throws IOException {
        if (inRamLimit < MIN_IN_RAM_LIMIT) {
            throw new IllegalArgumentException("inRamLimit is too small");
        }
        this.inRamLimit = (int) inRamLimit;
        this.fileChannel = (FileChannel) Files.newByteChannel(Path.of("biglist-data.bin"),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.segmentBuffer = ByteBuffer.allocate(segmentSize());
        this.longViewBuffer = segmentBuffer.asLongBuffer();
    }

    @Override
    public Long get(int index) {
        var localIndex = prepareLocalIndex(index, false);
        return longViewBuffer.get(localIndex);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Long set(int index, Long element) {
        var localIndex = prepareLocalIndex(index, false);
        var prevElement = longViewBuffer.get(localIndex);
        longViewBuffer.put(localIndex, element);
        return prevElement;
    }

    @Override
    public boolean add(Long element) {
        set(size++, element);
        return true;
    }

    @Override
    public void add(int index, Long element) {
        var localIndex = prepareLocalIndex(index, true);
        for (var i = index; i <= size; ++i, ++localIndex) {
            if (localIndex == inRamLimit) {
                localIndex = 0;
                switchSegment(activeSegment + 1);
            }
            var prevElement = longViewBuffer.get(localIndex);
            longViewBuffer.put(localIndex, element);
            element = prevElement;
        }
        ++size;
    }

    @Override
    public Long remove(int index) {
        var localIndex = prepareLocalIndex(size - 1, false);
        var element = longViewBuffer.get(localIndex);
        --localIndex;
        for (var i = size - 2; i >= index; --i, --localIndex) {
            if (localIndex == -1) {
                localIndex = inRamLimit - 1;
                switchSegment(activeSegment - 1);
            }
            var prevElement = longViewBuffer.get(localIndex);
            longViewBuffer.put(localIndex, element);
            element = prevElement;
        }
        --size;
        return element;
    }

    private int prepareLocalIndex(int index, boolean endAllowed) {
        if (index < 0 || index > size() || (index == size() && !endAllowed)) {
            throw new IndexOutOfBoundsException(index);
        }
        var segment = index / inRamLimit;
        if (segment != activeSegment) {
            switchSegment(segment);
        }
        return index % inRamLimit;
    }

    private void switchSegment(int segment) {
        storeActiveSegmentToDisk();
        activeSegment = segment;
        loadActiveSegmentFromDisk();
    }

    private void storeActiveSegmentToDisk() {
        try {
            segmentBuffer.rewind();
            fileChannel.write(segmentBuffer, segmentOffset(activeSegment));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadActiveSegmentFromDisk() {
        var offset = segmentOffset(activeSegment);
        try {
            if (fileChannel.size() >= offset + segmentSize()) {
                segmentBuffer.clear();
                fileChannel.read(segmentBuffer, offset);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private long segmentOffset(int segment) {
        return (long) segment * segmentSize();
    }

    private int segmentSize() {
        return inRamLimit * Long.BYTES;
    }
}

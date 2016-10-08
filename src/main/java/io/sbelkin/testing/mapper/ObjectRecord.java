package io.sbelkin.testing.mapper;

import org.easybatch.core.record.Header;
import org.easybatch.core.record.StringRecord;

/**
 * Created by sbelkin on 9/20/2016.
 */
public class ObjectRecord extends StringRecord {
    public ObjectRecord(Header header, String payload) {
        super(header, payload);
    }
}

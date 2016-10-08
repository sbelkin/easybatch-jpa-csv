package io.sbelkin.testing.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.easybatch.core.mapper.RecordMapper;
import org.easybatch.core.mapper.RecordMappingException;
import org.easybatch.core.record.GenericRecord;
import org.easybatch.core.util.Utils;
import org.easybatch.json.JsonRecord;

/**
 * Created by sbelkin on 9/20/2016.
 */

public class JavaObjectMapper<T> implements RecordMapper<ObjectRecord, GenericRecord<T>> {
    private Class<T> type;

    public JavaObjectMapper(Class<T> type) {
        Utils.checkNotNull(type, "target type");
        this.type = type;
    }

    public GenericRecord<T> processRecord(ObjectRecord objectRecord) throws RecordMappingException {
        try {
            return new GenericRecord(objectRecord.getHeader(), ((String)objectRecord.getPayload()).getBytes());
        } catch (Exception var3) {
            throw new RecordMappingException("Unable to map record " + objectRecord + " to target type", var3);
        }    }
}


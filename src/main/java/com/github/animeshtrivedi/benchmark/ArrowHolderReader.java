package com.github.animeshtrivedi.benchmark;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;

public class ArrowHolderReader extends ArrowReader {
    NullableFloat4Holder f4Holder;
    NullableFloat8Holder f8Holder;
    NullableIntHolder intHolder;
    NullableBigIntHolder longHolder;
    NullableVarBinaryHolder binHolder;

    protected ArrowHolderReader() {
        super();
        this.f4Holder = new NullableFloat4Holder();
        this.f8Holder = new NullableFloat8Holder();
        this.intHolder = new NullableIntHolder();
        this.longHolder =  new NullableBigIntHolder();
        this.binHolder = new NullableVarBinaryHolder();
    }

    final protected void consumeFloat4(Float4Vector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, f4Holder);
            if(f4Holder.isSet == 1){
                float4Count+=1;
                checksum+=f4Holder.value;
            }
        }
    }

    final protected void consumeFloat8(Float8Vector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, f8Holder);
            if(f8Holder.isSet == 1){
                float8Count+=1;
                checksum+=f8Holder.value;
            }
        }
    }

    final protected void consumeInt4(IntVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, intHolder);
            if(intHolder.isSet == 1){
                intCount+=1;
                checksum+=intHolder.value;
            }
        }
    }

    final protected void consumeBigInt(BigIntVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, longHolder);
            if(longHolder.isSet == 1){
                longCount+=1;
                checksum+=longHolder.value;
            }
        }
    }

    final protected void consumeBinary(VarBinaryVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, binHolder);
            if(binHolder.isSet == 1){
                binaryCount+=1;
                int length = binHolder.end  - binHolder.start;
                this.checksum+=length;
                this.binarySizeCount+=length;
            }
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.parquet.thrift;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.function.Supplier;

import org.apache.arrow.adapter.parquet.ParquetException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TIOStreamTransport;


/** Helpers to serialize / deserialize Thrift messages. */
public class ThriftHelpers {

  /** Serialize a Thrift message to an nio ByteChannel. */
  public static <T extends TBase<?, ?>> void serializeThriftMsg(T msg, WritableByteChannel channel, Object encryptor) {

    OutputStream stream = Channels.newOutputStream(channel);
    serializeThriftMsg(msg, stream, encryptor);
  }

  /** Serialize a Thrift message to an OutputStream. */
  public static <T extends TBase<?, ?>> void serializeThriftMsg(T msg, OutputStream stream, Object encryptor) {

    // thrift message is not encrypted
    if (encryptor == null) {

      serializeUnencryptedMsg(msg, stream);

    // thrift message is encrypted
    } else {

      // TODO: Encryption support, see CPP thrift_internal.h lin 410
      throw new ParquetException("Encryption support not implemented yet");
    }
  }

  /** Serialize an unencrypted Thrift message to an OutputStream. */
  public static <T extends TBase<?, ?>> void serializeUnencryptedMsg(T msg, OutputStream stream) {

    try {

      TIOStreamTransport thriftStream = new TIOStreamTransport(stream);

      // TODO: Check, is writing with the compact protocol correct?
      TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
      TProtocol thriftProtocol = protocolFactory.getProtocol(thriftStream);

      msg.write(thriftProtocol);

    } catch (TException e) {

      throw new ParquetException("Couldn't serialize thrift: " + e.getMessage(), e);
    }
  }

  /** Deserialize a Thrift message from an ArrowBuf. */
  public static <T extends TBase<?, ?>> T deserializeThriftMsg(
      Supplier<T> msgSupplier, ArrowBuf buffer, Object decryptor) {

    // thrift message is not encrypted
    if (decryptor == null) {

      return deserializeUnencryptedMsg(msgSupplier, buffer);

    // thrift message is encrypted
    } else {

      // TODO: Encryption support, see CPP thrift_internal.h lin 410
      throw new ParquetException("Encryption support not implemented yet");
    }
  }

  /** Deserialize an unencrypted Thrift message from an ArrowBuf. */
  public static <T extends TBase<?, ?>> T deserializeUnencryptedMsg(Supplier<T> msgSupplier, ArrowBuf buffer) {

    try {

      // We need a Java native buffer to pass into thrift
      // Since ArrowBuf sets up a new buffer around the underlying memory, reader index will not be updated
      // So, record the bytes read from tne nio buffer, to update the ArrowBuf later

      ByteBuffer nioBuffer = buffer.nioBuffer();
      long nioStart = nioBuffer.position();

      TByteBuffer thriftBuffer = new TByteBuffer(nioBuffer);
      TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
      TProtocol thriftProtocol = protocolFactory.getProtocol(thriftBuffer);

      T msg = msgSupplier.get();
      msg.read(thriftProtocol);

      long nioEnd = nioBuffer.position();
      buffer.readerIndex(buffer.readerIndex() + nioEnd - nioStart);

      return msg;

    } catch (TException e) {

      throw new ParquetException("Couldn't deserialize thrift: " + e.getMessage(), e);
    }
  }
}

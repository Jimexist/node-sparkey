
#include <node.h>
#include <nan.h>
#include <sparkey.h>
#include "writer.h"



namespace sparkey {

v8::Persistent<v8::FunctionTemplate> LogWriter::constructor;

/**
 * Open worker.
 */

class WriterOpenWorker : public Nan::AsyncWorker {
  public:
    WriterOpenWorker(
        LogWriter *self
      , int block_size
      , sparkey_compression_type compression_type
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback)
      , self(self)
      , block_size(block_size)
      , compression_type(compression_type) {}

    void
    Execute() {
      sparkey_returncode rc;
      rc = self->OpenWriter(block_size, compression_type);
      if (SPARKEY_SUCCESS != rc) {
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    LogWriter *self;
    int block_size;
    sparkey_compression_type compression_type;
};

/**
 * Append open worker.
 */

class WriterAppendWorker : public Nan::AsyncWorker {
  public:
    WriterAppendWorker(
        LogWriter *self
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback)
      , self(self) {}

    void
    Execute() {
      sparkey_returncode rc = self->OpenWriterForAppending();
      if (SPARKEY_SUCCESS != rc) {
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    LogWriter *self;
};

/**
 * Put worker.
 */

class WriterPutWorker : public Nan::AsyncWorker {
  public:
    WriterPutWorker(
        LogWriter *self
      , char *key
      , size_t keysize
      , char *value
      , size_t valuesize
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback)
      , self(self)
      , keysize(keysize)
      , valuesize(valuesize)
      , key(key)
      , value(value) {}

    void
    Execute() {
      sparkey_returncode rc = self->SetKey(keysize, key, valuesize, value);

      delete key;
      delete value;

      if (SPARKEY_SUCCESS != rc) {
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    LogWriter *self;
    size_t keysize;
    size_t valuesize;
    char *key;
    char *value;
};

/**
 * Delete worker.
 */

class WriterDeleteWorker : public Nan::AsyncWorker {
  public:
    WriterDeleteWorker(
        LogWriter *self
      , char *key
      , size_t keysize
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback)
      , self(self)
      , keysize(keysize)
      , key(key) {}

    void
    Execute() {
      sparkey_returncode rc = self->DeleteKey(keysize, key);

      delete key;

      if (SPARKEY_SUCCESS != rc) {
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    LogWriter *self;
    size_t keysize;
    char *key;
};

/**
 * Flush worker.
 */

class WriterFlushWorker : public Nan::AsyncWorker {
  public:
    WriterFlushWorker(
        LogWriter *self
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback)
      , self(self) {}

    void
    Execute() {
      sparkey_returncode rc = self->FlushWriter();
      if (SPARKEY_SUCCESS != rc) {
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    LogWriter *self;
};

/**
 * Close worker.
 */

class WriterCloseWorker : public Nan::AsyncWorker {
  public:
    WriterCloseWorker(
        LogWriter *self
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback)
      , self(self) {}

    void
    Execute() {
      sparkey_returncode rc = self->CloseWriter();
      if (SPARKEY_SUCCESS != rc) {
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    LogWriter *self;
};

LogWriter::LogWriter() {
  writer = NULL;
  is_open = false;
};

LogWriter::~LogWriter() {
  delete path;
};

sparkey_returncode
LogWriter::OpenWriter(int block_size, sparkey_compression_type type) {
  // noop if we're open
  if (is_open) return SPARKEY_SUCCESS;
  sparkey_returncode rc = sparkey_logwriter_create(
      &writer
    , path
    , type
    , block_size
  );
  if (SPARKEY_SUCCESS == rc) is_open = true;
  return rc;
}

sparkey_returncode
LogWriter::OpenWriterForAppending() {
  // XXX: sparkey lets you open an open log for appending
  // this is likely a bug.
  // TODO: guard
  sparkey_returncode rc = sparkey_logwriter_append(&writer, path);
  if (SPARKEY_SUCCESS == rc) is_open = true;
  return rc;
}

sparkey_returncode
LogWriter::SetKey(
    size_t keysize
  , const char *key
  , size_t valuesize
  , const char *value
  ) {
  // guard
  if (!is_open) return SPARKEY_LOG_CLOSED;
  return sparkey_logwriter_put(
      writer
    , keysize
    , (uint8_t *) key
    , valuesize
    , (uint8_t *) value
  );
}

sparkey_returncode
LogWriter::DeleteKey(size_t keysize, const char *key) {
  // guard
  if (!is_open) return SPARKEY_LOG_CLOSED;
  return sparkey_logwriter_delete(
      writer
    , keysize
    , (uint8_t *) key
  );
}

sparkey_returncode
LogWriter::FlushWriter() {
  if (!is_open) return SPARKEY_LOG_CLOSED;
  return sparkey_logwriter_flush(writer);
}

sparkey_returncode
LogWriter::CloseWriter() {
  // noop if we're closed
  if (!is_open) return SPARKEY_SUCCESS;
  sparkey_returncode rc = sparkey_logwriter_close(&writer);
  if (SPARKEY_SUCCESS == rc) is_open = false;
  return rc;
}

/*
 * Methods exposed to v8.
 */

void
LogWriter::Init(v8::Handle<v8::Object> exports) {
  v8::Local<v8::FunctionTemplate> tpl = v8::FunctionTemplate::New(
    LogWriter::New
  );
  NanAssignPersistent(v8::FunctionTemplate, constructor, tpl);

  tpl->SetClassName(Nan::Symbol("LogWriter"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  NODE_SET_PROTOTYPE_METHOD(tpl, "open", Open);
  NODE_SET_PROTOTYPE_METHOD(tpl, "openSync", OpenSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "append", Append);
  NODE_SET_PROTOTYPE_METHOD(tpl, "appendSync", AppendSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "close", Close);
  NODE_SET_PROTOTYPE_METHOD(tpl, "closeSync", CloseSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "put", Put);
  NODE_SET_PROTOTYPE_METHOD(tpl, "putSync", PutSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "flush", Flush);
  NODE_SET_PROTOTYPE_METHOD(tpl, "flushSync", FlushSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "delete", Delete);
  NODE_SET_PROTOTYPE_METHOD(tpl, "deleteSync", DeleteSync);

  exports->Set(Nan::Symbol("LogWriter"), tpl->GetFunction());
}

NAN_METHOD(LogWriter::New) {

  LogWriter *self = new LogWriter;
  size_t pathsize;
  self->path = Nan::Utf8String(info.args[0], &pathsize);
  self->Wrap(info.args.This());
  NanReturnValue(info.args.This());
}

NAN_METHOD(LogWriter::Open) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());

  v8::Local<v8::Object> options;
  v8::Local<v8::Function> fn;
  if (2 == info.args.Length()) {
    options = v8::Local<v8::Object>::Cast(info.args[0]);
    fn = info.args[1].As<v8::Function>();
  } else {
    fn = info.args[0].As<v8::Function>();
  }
  int block_size = 10;
  if (!options.IsEmpty() && options->Has(Nan::Symbol("blockSize"))) {
    block_size = options->Get(Nan::Symbol("blockSize"))->NumberValue();
  }
  bool use_snappy = NanBooleanOptionValue(
      options
    , Nan::Symbol("useSnappy")
    , true
  );
  sparkey_compression_type compression_type = use_snappy
    ? SPARKEY_COMPRESSION_SNAPPY
    : SPARKEY_COMPRESSION_NONE;
  WriterOpenWorker *worker = new WriterOpenWorker(
      self
    , block_size
    , compression_type
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::OpenSync) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  v8::Local<v8::Object> options = v8::Local<v8::Object>::Cast(info.args[0]);
  int block_size = 10;
  if (!options.IsEmpty() && options->Has(Nan::Symbol("blockSize"))) {
    block_size = options->Get(Nan::Symbol("blockSize"))->NumberValue();
  }
  bool use_snappy = NanBooleanOptionValue(
      options
    , Nan::Symbol("useSnappy")
    , true
  );
  sparkey_compression_type compression_type = use_snappy
    ? SPARKEY_COMPRESSION_SNAPPY
    : SPARKEY_COMPRESSION_NONE;
  sparkey_returncode rc = self->OpenWriter(block_size, compression_type);
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::Append) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  v8::Local<v8::Function> fn = info.args[0].As<v8::Function>();
  WriterAppendWorker *worker = new WriterAppendWorker(
      self
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::AppendSync) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  sparkey_returncode rc = self->OpenWriterForAppending();
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::Put) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  size_t keysize;
  size_t valuesize;
  char *key = Nan::Utf8String(info.args[0], &keysize);
  char *value = Nan::Utf8String(info.args[1], &valuesize);
  v8::Local<v8::Function> fn = info.args[2].As<v8::Function>();
  WriterPutWorker *worker = new WriterPutWorker(
      self
    , key
    , keysize
    , value
    , valuesize
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::PutSync) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  size_t keysize;
  size_t valuesize;
  char *key = Nan::Utf8String(info.args[0], &keysize);
  char *value = Nan::Utf8String(info.args[1], &valuesize);
  sparkey_returncode rc = self->SetKey(keysize, key, valuesize, value);

  delete key;
  delete value;

  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::Delete) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  size_t keysize;
  char *key = Nan::Utf8String(info.args[0], &keysize);
  v8::Local<v8::Function> fn = info.args[1].As<v8::Function>();
  WriterDeleteWorker *worker = new WriterDeleteWorker(
      self
    , key
    , keysize
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::DeleteSync) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  size_t keysize;
  char *key = Nan::Utf8String(info.args[0], &keysize);
  sparkey_returncode rc = self->DeleteKey(keysize, key);

  delete key;

  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::Flush) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  v8::Local<v8::Function> fn = info.args[0].As<v8::Function>();
  WriterFlushWorker *worker = new WriterFlushWorker(
      self
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::FlushSync) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  sparkey_returncode rc = self->FlushWriter();
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::Close) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  v8::Local<v8::Function> fn = info.args[0].As<v8::Function>();
  WriterCloseWorker *worker = new WriterCloseWorker(
      self
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogWriter::CloseSync) {

  LogWriter *self = node::ObjectWrap::Unwrap<LogWriter>(info.args.This());
  sparkey_returncode rc = self->CloseWriter();
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

} // namespace sparkey

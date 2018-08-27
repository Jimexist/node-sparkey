
#include <node.h>
#include <nan.h>
#include <sparkey.h>
#include "reader.h"
#include "iterator.h"



namespace sparkey {

v8::Persistent<v8::FunctionTemplate> LogReader::constructor;

/**
 * Close worker.
 */

class LogReaderCloseWorker : public Nan::AsyncWorker {
  public:
    LogReaderCloseWorker(
        LogReader *self
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback), self(self) {}

    void
    Execute() {
      self->CloseReader();
    };

  private:
    LogReader *self;
};

/**
 * Open worker.
 */

class LogReaderOpenWorker : public Nan::AsyncWorker {
  public:
    LogReaderOpenWorker(
        LogReader *self
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback), self(self) {}

    void
    Execute() {
      sparkey_returncode rc = self->OpenReader();
      if (SPARKEY_SUCCESS != rc) {
        // most obnixous part of NAN: they free `errmsg`, so
        // you've got to use a pointer you own :(
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    LogReader *self;
};

LogReader::LogReader() {
  reader = NULL;
  iterator = NULL;
  is_open = false;
}

LogReader::~LogReader() {
  delete path;
}

sparkey_returncode
LogReader::OpenReader() {
  // noop if we're already open
  if (is_open) return SPARKEY_SUCCESS;
  sparkey_returncode rc = sparkey_logreader_open(&reader, path);
  if (SPARKEY_SUCCESS == rc) is_open = true;
  return rc;
}

void
LogReader::CloseReader() {
  // noop if we're already closed
  if (!is_open) return;
  sparkey_logreader_close(&reader);
  is_open = false;
}

/*
 * Methods exposed to v8.
 */

void
LogReader::Init(v8::Handle<v8::Object> exports) {
  v8::Local<v8::FunctionTemplate> tpl = v8::FunctionTemplate::New(
    LogReader::New
  );
  NanAssignPersistent(v8::FunctionTemplate, constructor, tpl);

  tpl->SetClassName(Nan::Symbol("LogReader"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  NODE_SET_PROTOTYPE_METHOD(tpl, "close", Close);
  NODE_SET_PROTOTYPE_METHOD(tpl, "closeSync", CloseSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "open", Open);
  NODE_SET_PROTOTYPE_METHOD(tpl, "openSync", OpenSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "getBlockSize", GetBlockSize);
  NODE_SET_PROTOTYPE_METHOD(tpl, "iterator", NewIterator);

  exports->Set(Nan::Symbol("LogReader"), tpl->GetFunction());
}

NAN_METHOD(LogReader::New) {

  LogReader *self = new LogReader;
  size_t pathsize;
  self->path = Nan::Utf8String(info.args[0], &pathsize);
  self->block_size = -1;
  self->Wrap(info.args.This());
  NanReturnValue(info.args.This());
}

NAN_METHOD(LogReader::Close) {

  LogReader *self = ObjectWrap::Unwrap<LogReader>(info.args.This());
  v8::Local<v8::Function> fn = info.args[0].As<v8::Function>();
  LogReaderCloseWorker *worker = new LogReaderCloseWorker(
      self
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogReader::CloseSync) {

  LogReader *self = ObjectWrap::Unwrap<LogReader>(info.args.This());
  self->CloseReader();
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogReader::Open) {

  LogReader *self = ObjectWrap::Unwrap<LogReader>(info.args.This());
  v8::Local<v8::Function> fn = info.args[0].As<v8::Function>();
  LogReaderOpenWorker *worker = new LogReaderOpenWorker(
      self
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(LogReader::OpenSync) {

  LogReader *self = ObjectWrap::Unwrap<LogReader>(info.args.This());
  sparkey_returncode rc = self->OpenReader();
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

// TODO this should probably just be a getter function (reader.blockSize)
NAN_METHOD(LogReader::GetBlockSize) {

  LogReader *self = ObjectWrap::Unwrap<LogReader>(info.args.This());
  // only lookup once, as it's constant
  if (-1 == self->block_size) {
    self->block_size = sparkey_logreader_get_compression_blocksize(
      self->reader
    );
  }
  NanReturnValue(v8::Number::New(self->block_size));
}

NAN_METHOD(LogReader::NewIterator) {

  NanReturnValue(LogIterator::NewInstance(info.args.This()));
}

} // namespace sparkey

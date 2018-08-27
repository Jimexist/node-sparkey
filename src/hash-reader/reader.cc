
#include <node.h>
#include <nan.h>
#include <sparkey.h>
#include "reader.h"
#include "iterator.h"



namespace sparkey {

v8::Persistent<v8::FunctionTemplate> HashReader::constructor;

/**
 * Open worker.
 */

class HashReaderOpenWorker : public Nan::AsyncWorker {
  public:
    HashReaderOpenWorker(
        HashReader *self
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback), self(self) {}

    void SetError(sparkey_returncode rc) {
      errmsg_ = strdup(sparkey_errstring(rc));
    }

    void
    Execute() {
      sparkey_returncode rc = self->OpenReader();
      if (SPARKEY_SUCCESS != rc) {
        return SetError(rc);
      }
    }

  private:
    HashReader *self;
};

/**
 * Close worker.
 */

class HashReaderCloseWorker : public Nan::AsyncWorker {
  public:
    HashReaderCloseWorker(
        HashReader *self
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback), self(self) {}

    void
    Execute() {
      self->CloseReader();
    }

  private:
    HashReader *self;
};

HashReader::HashReader() {
  log_reader = NULL;
  hash_reader = NULL;
  is_open = false;
}

HashReader::~HashReader() {
  delete logfile;
  delete hashfile;
}

sparkey_returncode
HashReader::OpenReader() {
  if (is_open) return SPARKEY_SUCCESS;
  sparkey_returncode rc = sparkey_hash_open(
      &hash_reader
    , hashfile
    , logfile
  );
  if (SPARKEY_SUCCESS == rc) is_open = true;
  return rc;
}

void
HashReader::CloseReader() {
  if (!is_open) return;
  sparkey_hash_close(&hash_reader);
  is_open = false;
}

/*
 * Methods exposed to v8.
 */

void
HashReader::Init(v8::Handle<v8::Object> exports) {
  v8::Local<v8::FunctionTemplate> tpl = v8::FunctionTemplate::New(
    HashReader::New
  );
  NanAssignPersistent(v8::FunctionTemplate, constructor, tpl);

  tpl->SetClassName(Nan::Symbol("HashReader"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  NODE_SET_PROTOTYPE_METHOD(tpl, "close", Close);
  NODE_SET_PROTOTYPE_METHOD(tpl, "closeSync", CloseSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "open", Open);
  NODE_SET_PROTOTYPE_METHOD(tpl, "openSync", OpenSync);
  NODE_SET_PROTOTYPE_METHOD(tpl, "count", Count);
  NODE_SET_PROTOTYPE_METHOD(tpl, "iterator", NewIterator);

  exports->Set(Nan::Symbol("HashReader"), tpl->GetFunction());
}

NAN_METHOD(HashReader::New) {
  HashReader *self = new HashReader;
  size_t hashsize;
  size_t logsize;
  self->hashfile = Nan::Utf8String(info.args[0], &hashsize);
  self->logfile = Nan::Utf8String(info.args[1], &logsize);
  self->Wrap(info.args.This());
  NanReturnValue(info.args.This());
}

NAN_METHOD(HashReader::Open) {
  HashReader *self = ObjectWrap::Unwrap<HashReader>(info.args.This());
  v8::Local<v8::Function> fn = info.args[0].As<v8::Function>();
  HashReaderOpenWorker *worker = new HashReaderOpenWorker(
      self
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(HashReader::OpenSync) {
  HashReader *self = ObjectWrap::Unwrap<HashReader>(info.args.This());
  sparkey_returncode rc = self->OpenReader();
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  Nan::ReturnValue();
}

NAN_METHOD(HashReader::Close) {
  HashReader *self = ObjectWrap::Unwrap<HashReader>(info.args.This());
  v8::Local<v8::Function> fn = info.args[0].As<v8::Function>();
  HashReaderCloseWorker *worker = new HashReaderCloseWorker(
      self
    , new Nan::Callback(fn)
  );
  NanAsyncQueueWorker(worker);
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(HashReader::CloseSync) {
  HashReader *self = ObjectWrap::Unwrap<HashReader>(info.args.This());
  self->CloseReader();
  info.GetReturnValue().SetUndefined();
}

NAN_METHOD(HashReader::Count) {
  HashReader *self = ObjectWrap::Unwrap<HashReader>(info.args.This());
  uint64_t count = sparkey_hash_numentries(self->hash_reader);
  NanReturnValue(v8::Number::New(count));
}

NAN_METHOD(HashReader::NewIterator) {
  NanReturnValue(
    HashIterator::NewInstance(
      info.args.This()
    )
  );
}

} // namespace sparkey

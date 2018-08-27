
#include <node.h>
#include <nan.h>
#include <sparkey.h>
#include "hash.h"

namespace sparkey {

class HashWorker : public Nan::AsyncWorker {
  public:
    HashWorker(
        char *log_file
      , char *hash_file
      , int hash_size
      , Nan::Callback *callback
    ) : Nan::AsyncWorker(callback)
      , log_file(log_file)
      , hash_file(hash_file)
      , hash_size(hash_size) {}

    void
    Execute() {
      sparkey_returncode rc;
      rc = sparkey_hash_write(hash_file, log_file, hash_size);
      if (SPARKEY_SUCCESS != rc) {
        errmsg_ = strdup(sparkey_errstring(rc));
      }
    }

  private:
    char *log_file;
    char *hash_file;
    int hash_size;
};

NAN_METHOD(Hash) {
  size_t logsize;
  size_t hashsize;
  char *log_file = Nan::Utf8String(info.args[0], &logsize);
  char *hash_file = Nan::Utf8String(info.args[1], &hashsize);
  v8::Local<v8::Function> fn;
  int hash_size = 0;

  if (3 == info.args.Length()) {
    fn = info.args[2].As<v8::Function>();
  } else {
    hash_size = info.args[2]->NumberValue();
    fn = info.args[3].As<v8::Function>();
  }

  HashWorker *worker = new HashWorker(
      log_file
    , hash_file
    , hash_size
    , new Nan::Callback(fn)
  );
  AsyncQueueWorker(worker);
  Nan::info.GetReturnValue().SetUndefined();
}

NAN_METHOD(HashSync) {
  size_t logsize;
  size_t hashsize;
  char *log_file = Nan::Utf8String(info.args[0], &logsize);
  char *hash_file = Nan::Utf8String(info.args[1], &hashsize);
  int hash_size = 3 == info.args.Length()
    ? info.args[2]->NumberValue()
    : 0;
  sparkey_returncode rc = sparkey_hash_write(
      hash_file
    , log_file
    , hash_size
  );
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  info.GetReturnValue().SetUndefined();
}

void
InitHash(v8::Handle<v8::Object> exports) {
  exports->Set(Nan::Symbol("hash"), v8::FunctionTemplate::New(
    Hash)->GetFunction()
  );
  exports->Set(Nan::Symbol("hashSync"), v8::FunctionTemplate::New(
    HashSync)->GetFunction()
  );
}

} // namespace sparkey

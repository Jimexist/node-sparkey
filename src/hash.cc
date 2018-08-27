
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
        errmsg = strdup(sparkey_errstring(rc));
      }
    }

  private:
    char *log_file;
    char *hash_file;
    int hash_size;
};

NAN_METHOD(Hash) {
  NanScope();
  size_t logsize;
  size_t hashsize;
  char *log_file = NanUtf8String(args[0], &logsize);
  char *hash_file = NanUtf8String(args[1], &hashsize);
  v8::Local<v8::Function> fn;
  int hash_size = 0;

  if (3 == args.Length()) {
    fn = args[2].As<v8::Function>();
  } else {
    hash_size = args[2]->NumberValue();
    fn = args[3].As<v8::Function>();
  }

  HashWorker *worker = new HashWorker(
      log_file
    , hash_file
    , hash_size
    , new Nan::Callback(fn)
  );
  AsyncQueueWorker(worker);
  Nan::ReturnUndefined();
}

NAN_METHOD(HashSync) {
  NanScope();
  size_t logsize;
  size_t hashsize;
  char *log_file = NanUtf8String(args[0], &logsize);
  char *hash_file = NanUtf8String(args[1], &hashsize);
  int hash_size = 3 == args.Length()
    ? args[2]->NumberValue()
    : 0;
  sparkey_returncode rc = sparkey_hash_write(
      hash_file
    , log_file
    , hash_size
  );
  if (SPARKEY_SUCCESS != rc) {
    Nan::ThrowError(sparkey_errstring(rc));
  }
  ReturnUndefined();
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

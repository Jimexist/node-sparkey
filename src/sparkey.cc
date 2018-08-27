
#include <node.h>
#include <v8.h>
#include <sparkey.h>
#include "log-writer/writer.h"
#include "log-reader/reader.h"
#include "log-reader/iterator.h"
#include "hash-reader/reader.h"
#include "hash-reader/iterator.h"
#include "hash.h"

NAN_MODULE_INIT(InitSparkey) {
  sparkey::LogWriter::Init(target);
  sparkey::LogReader::Init(target);
  sparkey::LogIterator::Init();
  sparkey::HashReader::Init(target);
  sparkey::HashIterator::Init();
  sparkey::InitHash(target);

  Nan::Set(target
    , Nan::New<v8::String>("SPARKEY_ENTRY_PUT").ToLocalChecked()
    , Nan::New<v8::Number>(SPARKEY_ENTRY_PUT)
  );

  Nan::Set(target
    , Nan::New<v8::String>("SPARKEY_ENTRY_DELETE").ToLocalChecked()
    , Nan::New<v8::Number>(SPARKEY_ENTRY_DELETE)
  );
}

NODE_MODULE(sparkey, InitSparkey)

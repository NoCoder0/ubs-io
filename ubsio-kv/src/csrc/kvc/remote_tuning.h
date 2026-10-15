#ifndef UBSIO_SWEEP_REMOTE_TUNING_H
#define UBSIO_SWEEP_REMOTE_TUNING_H
#include <cstdlib>
#include <fstream>
namespace ock { namespace ubsio {
struct RemoteTuning { size_t workers=2; bool manifestOneGet=false; };
inline bool ReadRemoteTuning(RemoteTuning &value, size_t defaultWorkers=2)
{
    value.workers=defaultWorkers;
    const char *path=std::getenv("UBSIO_KVC_TUNING_FILE");
    if (path==nullptr) return true;
    std::ifstream stream(path);
    size_t workers=0; unsigned manifest=2; std::string extra;
    if (!(stream >> workers >> manifest) || (stream >> extra) ||
        (workers!=2 && workers!=4) || manifest>1) return false;
    value.workers=workers; value.manifestOneGet=manifest==1;
    return true;
}
}}
#endif

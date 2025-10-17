
from dataflow.boot import ApplicationBoot
from dataflow.utils.utils import parse_long_args,set_cn_timezone

set_cn_timezone()
    
if __name__ == "__main__":
    ApplicationBoot.Start(configuration=parse_long_args())

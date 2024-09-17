using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.RocketMQ.Models
{
    public enum ConsumeFromWheres
    {
        CONSUME_FROM_LAST_OFFSET = 0,
        CONSUME_FROM_FIRST_OFFSET = 1,
        CONSUME_FROM_TIMESTAMP = 2,
        CONSUME_FROM_MIN_OFFSET = 3,
        CONSUME_FROM_MAX_OFFSET = 4,
    }
}
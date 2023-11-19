## PATH
ROCKSDB_PATH=/home/femu/CAZA/rocksdb
RESULT_DIR_ROOT_PATH=/home/femu/access_testdata
LOG_PATH=~/log
RAW_ZNS=nvme0n1
RAW_ZNS_PATH=/sys/block/${RAW_ZNS}/queue/scheduler


## Try until db_bench success
RETRY=1

## Algorithm
EAGER=0
LOG=3
LINEAR=4
EXP=9
#define RUNTIME_ZONE_RESET_DISABLED 0    		// |      x     |       x       |
#define RUNTIME_ZONE_RESET_ONLY 1        		// |      o     |       x       |
#define PARTIAL_RESET_WITH_ZONE_RESET 2  		// |      o     |       o       |
#define PARTIAL_RESET_ONLY 3             		// |      x     |       o       |
#define PARTIAL_RESET_AT_BACKGROUND 4    		// |      ?     |       o       |
#define PARTIAL_RESET_BACKGROUND_T_WITH_ZONE_RESET 5	// |      o     |       o       |
RUNTIME_ZONE_RESET_DISABLED=0
RUNTIME_ZONE_RESET_ONLY=1
PARTIAL_RESET_WITH_ZONE_RESET=2
PARTIAL_RESET_ONLY=3
PARTIAL_RESET_AT_BACKGROUND=4
PARTIAL_RESET_BACKGROUND_T_WITH_ZONE_RESET=5
PROACTIVE_ZC=6  

## Variations of FAR



## Dataset
G20=20971520
G24=25165824
G28=29360128
G32=33554432
G36=37748736
G40=41943040
G44=46137344
G48=50331648
G52=54525952
G56=58720256
G72=75497472
MINY=$G20
MINY2=$G24
SMALL=$G32 # 32
MED=$G36 # 36
LARGE=$G40 # 40
HEAVY=$G72
# 56gb 58720256
# 48gb 50331648
# 36gb 37748736
## Tuning Point
T=100
T_COMPACTION=3
T_SUBCOMPACTION=4
T_FLUSH=1
ZC_KICKS=15
UNTIL=15

SLOWDOWN_TRIGGER=16
STOPS_TRIGGER=16
SIZE=$HEAVY
# MOTIV_SMALL_ME_256MB_ERASEBLOCK_64MB
# FAR_LARGE_ME_256MB_ERASEBLOCK_64MB3

LIZA=0
CAZA=1

BASELINE_COMPACTION=0
MAX_INVALIDATION_COMPACTION=1

while :
do
    FAILED=0
    # for ALGORITHM in $RUNTIME_ZONE_RESET_ONLY $PARTIAL_RESET_WITH_ZONE_RESET
    for ALGORITHM in $LIZA $CAZA
    do
        for i in 1 2 3 4 5
        do
        if [ $ALGORITHM -eq $LIZA ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/LIZA
        elif [ $ALGORITHM -eq $CAZA ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/CAZA
        # elif [ $ALGORITHM -eq $RUNTIME_ZONE_RESET_ONLY ]; then
        #     RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/RUNTIME_ZONE_RESET_ONLY
        # elif [ $ALGORITHM -eq $PROACTIVE_ZC ]; then
        #     RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/PROACTIVE_ZC
        # elif [ $ALGORITHM -eq $RUNTIME_ZONE_RESET_DISABLED ]; then
        #     RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/noruntime
        else 
            echo "No such Algorithm"
            exit
        fi
        
        if [ ! -d ${RESULT_DIR_PATH} ] 
        then
            echo "NO ${RESULT_DIR_PATH}"
            mkdir ${RESULT_DIR_PATH}
        fi
            for FUNCTION in $BASELINE_COMPACTION $MAX_INVALIDATION_COMPACTION
                do
                    if [ $FUNCTION -eq $BASELINE_COMPACTION ]; then
                        RESULT_PATH=${RESULT_DIR_PATH}/result_${T}_${SIZE}_BASELINE_${i}.txt
                    elif [ $FUNCTION -eq $MAX_INVALIDATION_COMPACTION ]; then
                        RESULT_PATH=${RESULT_DIR_PATH}/result_${T}_${SIZE}_MAX_INVALIDATION_${i}.txt
                    # elif [ $FUNCTION -eq $EXP ]; then
                    #     RESULT_PATH=${RESULT_DIR_PATH}/result_${T}_${SIZE}_EXP_${i}.txt
                    # elif [ $FUNCTION -eq $EAGER ]; then
                    #     RESULT_PATH=${RESULT_DIR_PATH}/result_${T}_${SIZE}_EAGER_${i}.txt
                    else  
                        echo "error"
                    fi
                    # RESULT_PATH=${RESULT_DIR_PATH}/result_${T}_${SIZE}_${i}.txt
                    while :
                    do

                        if [ -f ${RESULT_PATH} ]; then
                            echo "already $RESULT_PATH exists"
                            break
                        fi

                        sleep 5
                        ## Initialize ZenFS
                        echo "mq-deadline" | sudo tee ${RAW_ZNS_PATH}
                        sudo rm -rf ${LOG_PATH}
                        mkdir ${LOG_PATH}
                        sudo ${ROCKSDB_PATH}/plugin/zenfs/util/zenfs mkfs --force --enable_gc --zbd=/${RAW_ZNS} --aux_path=${LOG_PATH} > ./mkfs

                        EC=$?
                        if [ $EC -eq 254 ]; then
                            echo "Failed to open device"
                            exit
                        fi
                        sleep 3
                        echo $RESULT_PATH
                        sudo ${ROCKSDB_PATH}/db_bench \
                        -num=${SIZE} -benchmarks="fillrandom,stats" --fs_uri=zenfs://dev:nvme0n1 -statistics  -value_size=1024 \
                          -max_background_compactions=${T_COMPACTION}   -max_background_flushes=${T_FLUSH} -subcompactions=${T_SUBCOMPACTION}  \
                          -histogram -seed=1699101730035899  \
                        -reset_scheme=0 -tuning_point=100 -partial_reset_scheme=1 -disable_wal=true -zc=${ZC_KICKS} -until=${UNTIL} \
                        -allocation_scheme=0 -compaction_scheme=1 > ${RESULT_DIR_PATH}/tmp
                        EC=$?
                        if grep -q "${SIZE} operations;" ${RESULT_DIR_PATH}/tmp; then
                            cat ${RESULT_DIR_PATH}/tmp > ${RESULT_PATH}
                            rm -rf ${RESULT_DIR_PATH}/tmp
                            break
                        else
                            cat ${RESULT_DIR_PATH}/tmp > ${RESULT_DIR_PATH}/failed
                        fi
                        FAILED=1
                        if [ $RETRY -eq 1 ]; then
                            echo "${RESULT_PATH} failed, RETRY"
                        else
                            echo "${RESULT_PATH} failed"
                            break
                        fi   
                        sleep 15

                        if [ $EC -eq 254 ]; then
                            echo "Failed to open device"
                            exit
                        fi
                    done
                done
            sleep 10
        done
#        sleep 10
#        sudo /home/femu/dummy 1 1
#        sudo /home/femu/dummy 999 999
    done

    if [ $FAILED -eq 0 ]; then
        break
    fi
done

sudo /home/femu/dummy2 111 111
sudo ${RESULT_DIR_ROOT_PATH}/sendresultmail

## PATH
ROCKSDB_PATH=/home/sungjin/ZC_SMR/rocksdb
RESULT_DIR_ROOT_PATH=/home/sungjin/access_testdata
LOG_PATH=~/log
RAW_ZNS=sdb
RAW_ZNS_PATH=/sys/block/${RAW_ZNS}/queue/scheduler


## Try until db_bench success
RETRY=1

## ALLOCATION_ALGORITHM
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
T_COMPACTION=1
T_SUBCOMPACTION=1
T_FLUSH=1
ZC_KICKS=20
UNTIL=20

# SLOWDOWN_TRIGGER=16
# STOPS_TRIGGER=16
SIZE=$G48
# SIZE=83047219
# MOTIV_SMALL_ME_256MB_ERASEBLOCK_64MB
# FAR_LARGE_ME_256MB_ERASEBLOCK_64MB3

LIZA=0
CAZA=1
CAZA_ADV=2

BASELINE_COMPACTION=0
MAX_INVALIDATION_COMPACTION=1



MAX_COMPACTION_KICK=25
MAX_COMPACTION_START_LEVEL=2

INPUT_AWARE_SCHEME=0

MB256=268435456
MB1=1048576

READ_THEN_WRITE=0
WRITE_AFTER_READ_DONE=1



while :
do
    FAILED=0
    for ZC_SCHEME in  $READ_THEN_WRITE $WRITE_AFTER_READ_DONE
    do
        for i in 1
        do
        if [ $ZC_SCHEME -eq $READ_THEN_WRITE ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/READ_THEN_WRITE
        elif [ $ZC_SCHEME -eq $WRITE_AFTER_READ_DONE ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/WRITE_AFTER_READ_DONE
        # elif [ $ALLOCATION_ALGORITHM -eq $CAZA_ADV ]; then
        #     RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/CAZA_ADV
        #     MAX_COMPACTION_KICK=22
        # elif [ $ALLOCATION_ALGORITHM -eq $RUNTIME_ZONE_RESET_ONLY ]; then
        #     RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/RUNTIME_ZONE_RESET_ONLY
        # elif [ $ALLOCATION_ALGORITHM -eq $PROACTIVE_ZC ]; then
        #     RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/PROACTIVE_ZC
        # elif [ $ALLOCATION_ALGORITHM -eq $RUNTIME_ZONE_RESET_DISABLED ]; then
        #     RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/noruntime
        else 
            echo "No such ALLOCATION_ALGORITHM"
            exit
        fi
        
        if [ ! -d ${RESULT_DIR_PATH} ] 
        then
            echo "NO ${RESULT_DIR_PATH}"
            mkdir ${RESULT_DIR_PATH}
        fi
            for IO_SIZE in $MB256 $MB1
                do
                    if [ $IO_SIZE -eq $MB256 ]; then
                        RESULT_PATH=${RESULT_DIR_PATH}/result_${SIZE}_256MB_${T_FLUSH}_${T_COMPACTION}_${T_SUBCOMPACTION}_${i}
                    elif [ $IO_SIZE -eq $MB1 ]; then
                        RESULT_PATH=${RESULT_DIR_PATH}/result_${SIZE}_1MB_${T_FLUSH}_${T_COMPACTION}_${T_SUBCOMPACTION}_${i}
                    # elif [ $COMPACTION_ALGORITHM -eq $EXP ]; then
                    #     RESULT_PATH=${RESULT_DIR_PATH}/result_${T}_${SIZE}_EXP_${i}.txt
                    # elif [ $COMPACTION_ALGORITHM -eq $EAGER ]; then
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
                         -num=${SIZE} -benchmarks="fillrandom,stats" --fs_uri=zenfs://dev:sdb -statistics  -value_size=1024 \
                          -max_background_compactions=${T_COMPACTION}   -max_background_flushes=${T_FLUSH} -subcompactions=${T_SUBCOMPACTION}  \
                          -histogram -seed=1699101730035899  -wait_for_compactions=false -enable_intraL0_compaction=false \
                        -reset_scheme=0 -tuning_point=100 -partial_reset_scheme=1 -disable_wal=false -zc=${ZC_KICKS} -until=${UNTIL} \
                        -allocation_scheme=0  -compaction_scheme=0 \
                         -max_compaction_start_level=5 -input_aware_scheme=0 \
                        -max_compaction_kick=0 -default_extent_size=${IO_SIZE} -async_zc_enabled=${ZC_SCHEME} > ${RESULT_DIR_PATH}/tmp

                        # -num=${SIZE} -benchmarks="fillrandom,stats" --fs_uri=zenfs://dev:sdb -statistics  -value_size=1024 \
                        #   -max_background_compactions=${T_COMPACTION}   -max_background_flushes=${T_FLUSH} -subcompactions=${T_SUBCOMPACTION}  \
                        #   -histogram -seed=1699101730035899  -wait_for_compactions=false -enable_intraL0_compaction=false \
                        # -reset_scheme=0 -tuning_point=100 -partial_reset_scheme=1 -disable_wal=true -zc=${ZC_KICKS} -until=${UNTIL} \
                        # -allocation_scheme=${ALLOCATION_ALGORITHM}  -compaction_scheme=${COMPACTION_ALGORITHM} \
                        #  -max_compaction_start_level=${MAX_COMPACTION_START_LEVEL} -input_aware_scheme=${MAX_INVALIDATION_COMPACTION}  \
                        # -max_compaction_kick=${MAX_COMPACTION_KICK} > ${RESULT_DIR_PATH}/tmp
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
#        sudo /home/sungjin/dummy 1 1
#        sudo /home/sungjin/dummy 999 999
    done

    if [ $FAILED -eq 0 ]; then
        break
    fi
done

sudo /home/sungjin/dummy2 111 111
sudo ${RESULT_DIR_ROOT_PATH}/sendresultmail

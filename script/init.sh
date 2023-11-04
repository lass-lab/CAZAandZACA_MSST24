# ZNS_SCHEDULER=/sys/block/nvme0n1/queue/scheduler
# ROCKSDB_PATH=/home/gs201/Desktop/rocksdb
# LOG_PATH=/home/gs201/log_
# RAW_ZNS=/nvme0n1

sudo rm -rf ~/log
sudo mkdir -p ~/log

echo "mq-deadline" | sudo tee /sys/block/nvme0n1/queue/scheduler

sudo /home/femu/DeviceSideCircular/rocksdb/plugin/zenfs/util/zenfs mkfs --force --enable_gc   --zbd=/nvme0n1 --aux_path=/home/femu/log


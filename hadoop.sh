#!/bin/bash

#=============================================================================
# Hadoop 集群自动部署脚本 (虚拟机版本)
# 适用于: Ubuntu 24.04 + Hadoop 3.4.2 + OpenJDK 17
# 使用方法: bash hadoop_deploy.sh
#=============================================================================

set -e # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 全局变量
HADOOP_VERSION="3.4.2"
HADOOP_USER="hadoop"
HADOOP_PASSWORD=""
CURRENT_USER=""
CURRENT_USER_PASSWORD=""
MASTER_IP=""
MASTER_HOSTNAME="hadoop01"
SLAVE_COUNT=0
declare -a SLAVE_IPS
declare -a SLAVE_HOSTNAMES

#=============================================================================
# 工具函数
#=============================================================================

print_info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
  echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
  echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
  echo -e "\n${GREEN}========================================${NC}"
  echo -e "${GREEN}$1${NC}"
  echo -e "${GREEN}========================================${NC}\n"
}

# 在远程节点执行命令
exec_remote() {
  local host=$1
  local cmd=$2
  local user=$3
  local pwd=$4

  # 如果命令中包含 sudo，则通过 echo 密码 | sudo -S 执行
  if [[ "$cmd" == *"sudo"* ]]; then
    sshpass -p "$pwd" ssh -o StrictHostKeyChecking=no ${user}@${host} \
      "echo \"$pwd\" | sudo -S bash -c '$cmd'"
  else
    sshpass -p "$pwd" ssh -o StrictHostKeyChecking=no ${user}@${host} "$cmd"
  fi
}

# 向远程节点复制文件
copy_to_remote() {
  local host=$1
  local src=$2
  local dest=$3
  sshpass -p "$CURRENT_USER_PASSWORD" rsync -avz --progress -e "ssh -o StrictHostKeyChecking=no" "$src" ${CURRENT_USER}@${host}:"$dest"
}

#=============================================================================
# 用户输入收集
#=============================================================================

collect_cluster_info() {
  print_step "步骤 1: 收集集群信息"

  # 获取当前用户信息
  CURRENT_USER=$(whoami)
  print_info "当前用户: $CURRENT_USER"

  # 获取当前用户密码（用于SSH到其他节点）
  echo -n "请输入当前用户($CURRENT_USER)在所有节点上的密码: "
  read -s CURRENT_USER_PASSWORD
  echo ""

  # 获取Master节点IP
  echo -n "请输入Master节点的IP地址: "
  read MASTER_IP

  # 获取Slave节点数量
  echo -n "请输入Slave节点数量 (建议2-10个): "
  read SLAVE_COUNT

  if [ "$SLAVE_COUNT" -lt 1 ]; then
    print_error "至少需要1个Slave节点"
    exit 1
  fi

  # 获取每个Slave节点的IP
  for i in $(seq 1 $SLAVE_COUNT); do
    local num=$(printf "%02d" $((i + 1)))
    echo -n "请输入Slave节点 $i 的IP地址: "
    read slave_ip
    SLAVE_IPS+=("$slave_ip")
    SLAVE_HOSTNAMES+=("hadoop${num}")
  done

  # 获取hadoop用户密码
  echo -n "请输入要为hadoop用户设置的密码 (所有节点使用相同密码): "
  read -s HADOOP_PASSWORD
  echo ""

  # 确认信息
  echo -e "\n${YELLOW}=== 集群配置确认 ===${NC}"
  echo "当前操作用户: $CURRENT_USER"
  echo "Master节点: $MASTER_HOSTNAME ($MASTER_IP)"
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    echo "Slave节点 $((i + 1)): ${SLAVE_HOSTNAMES[$i]} (${SLAVE_IPS[$i]})"
  done
  echo ""
  echo -n "确认以上信息正确? (yes/no): "
  read confirm
  if [ "$confirm" != "yes" ]; then
    print_error "部署已取消"
    exit 1
  fi
}

#=============================================================================
# 所有节点的基础配置（重构版，不再上传脚本）
#=============================================================================

configure_all_nodes() {
  print_step "步骤 2: 配置所有节点的基础环境"

  # 配置 Master 节点
  print_info "正在配置 Master 节点: $MASTER_HOSTNAME ($MASTER_IP)"
  configure_single_node "$MASTER_IP" "$MASTER_HOSTNAME" "true"

  # 配置所有 Slave 节点
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    print_info "正在配置 Slave 节点: ${SLAVE_HOSTNAMES[$i]} (${SLAVE_IPS[$i]})"
    configure_single_node "${SLAVE_IPS[$i]}" "${SLAVE_HOSTNAMES[$i]}" "false"
  done

  print_success "所有节点基础配置完成"
}

configure_single_node() {
  local node_ip=$1
  local hostname=$2
  local is_master=$3

  print_info "[$hostname] 开始基础配置"

  # =====================================================
  # 1. 创建 hadoop 用户
  # =====================================================
  print_info "[$hostname] 检查 hadoop 用户是否存在..."
  exec_remote "$node_ip" "
    if id '$HADOOP_USER' &>/dev/null; then
      echo '[INFO] 用户已存在，跳过创建步骤。'
    else
      echo '[INFO] 创建 Hadoop 用户: $HADOOP_USER'
      sudo useradd -m -s /bin/bash '$HADOOP_USER'
      echo '$HADOOP_USER:$HADOOP_PASSWORD' | sudo chpasswd
      sudo usermod -aG sudo '$HADOOP_USER'
      echo '$HADOOP_USER ALL=\(ALL\) NOPASSWD:ALL' | sudo tee /etc/sudoers.d/$HADOOP_USER >/dev/null
      sudo chmod 0440 /etc/sudoers.d/$HADOOP_USER
      echo '[OK] 用户 $HADOOP_USER 创建完成'
    fi
  " "$CURRENT_USER" "$CURRENT_USER_PASSWORD"

  # =====================================================
  # 2. 设置主机名
  # =====================================================
  print_info "[$hostname] 设置主机名..."
  exec_remote "$node_ip" "
    current_hostname=\$(hostname)
    if [[ \"\$current_hostname\" != \"$hostname\" ]]; then
      echo '[INFO] 更新主机名为 $hostname'
      sudo hostnamectl set-hostname '$hostname'
    else
      echo '[INFO] 主机名已为 $hostname，无需更改'
    fi
  " "$CURRENT_USER" "$CURRENT_USER_PASSWORD"

  # =====================================================
  # 3. 安装基础软件
  # =====================================================
  print_info "[$hostname] 安装必要软件..."
  exec_remote "$node_ip" "
    sudo apt-get update -y
    sudo apt-get install -y wget ssh sshpass vim net-tools
  " "$CURRENT_USER" "$CURRENT_USER_PASSWORD"

  # =====================================================
  # 4. 启动 SSH 服务
  # =====================================================
  print_info "[$hostname] 启动并启用 SSH 服务..."
  exec_remote "$node_ip" "
    sudo systemctl enable ssh
    sudo systemctl restart ssh
  " "$CURRENT_USER" "$CURRENT_USER_PASSWORD"

  # =====================================================
  # 5. 检测 Java 与 Hadoop 安装
  # =====================================================
  print_info "[$hostname] 检查 Java 与 Hadoop 安装状态..."

  jdk_installed=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${node_ip} \
    "[ -d /usr/lib/jvm/jdk-17.0.12-oracle-x64 ] && echo 'yes' || echo 'no'")

  hadoop_installed=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${node_ip} \
    "[ -d /usr/local/hadoop ] && [ -f /usr/local/hadoop/etc/hadoop/hadoop-env.sh ] && echo 'yes' || echo 'no'")

  if [[ "$jdk_installed" == "yes" ]]; then
    print_success "[$hostname] 检测到 Java 17 已安装"
  else
    print_info "[$hostname] Java 17 未安装，将在后续步骤安装"
  fi

  if [[ "$hadoop_installed" == "yes" ]]; then
    print_success "[$hostname] 检测到 Hadoop 已安装"
  else
    print_info "[$hostname] Hadoop 未安装，将在后续步骤安装"
  fi

  # =====================================================
  # 6. 分发 JDK 安装包
  # =====================================================
  local dir="$(dirname "$0")"
  local jdk_file="$dir/jdk-17.0.12_linux-x64_bin.deb"

  if [[ -f "$jdk_file" ]]; then
    print_info "[$hostname] 分发 JDK 安装包..."
    sshpass -p "$HADOOP_PASSWORD" rsync -avz --progress -e "ssh -o StrictHostKeyChecking=no" "$jdk_file" ${HADOOP_USER}@${node_ip}:/home/${HADOOP_USER}/
    if [[ $? -eq 0 ]]; then
      print_success "[$hostname] $(basename "$jdk_file") 传输完成"
    else
      print_error "[$hostname] $(basename "$jdk_file") 传输失败"
      exit 1
    fi
  else
    print_warning "[$hostname] 未找到 JDK 安装包，跳过传输"
  fi

  # =====================================================
  # 7. 安装 JDK 并配置环境变量
  # =====================================================
  if [[ "$jdk_installed" != "yes" ]]; then
    print_info "[$hostname] 安装 JDK 并配置环境变量..."
    exec_remote "$node_ip" "
      set -e
      sudo dpkg -i ~/jdk-17.0.12_linux-x64_bin.deb
      grep -q 'JAVA_HOME' ~/.bashrc || {
        echo 'export JAVA_HOME=/usr/lib/jvm/jdk-17.0.12-oracle-x64' >> ~/.bashrc
        echo 'export PATH=\$PATH:\$JAVA_HOME/bin' >> ~/.bashrc
      }
    " "$HADOOP_USER" "$HADOOP_PASSWORD"
  fi

  print_success "[$hostname] 节点基础配置完成"
}

#=============================================================================
# 配置hosts文件
#=============================================================================

configure_hosts_file() {
  print_step "步骤 3: 配置所有节点的hosts文件"

  # 生成hosts内容
  local hosts_content="127.0.0.1 localhost\n$MASTER_IP $MASTER_HOSTNAME\n"
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    hosts_content+="${SLAVE_IPS[$i]} ${SLAVE_HOSTNAMES[$i]}\n"
  done

  # 更新所有节点的hosts文件
  print_info "更新Master节点hosts文件"
  exec_remote "$MASTER_IP" "echo -e '$hosts_content' | sudo tee /etc/hosts > /dev/null" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"

  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    print_info "更新${SLAVE_HOSTNAMES[$i]}节点hosts文件"
    exec_remote "${SLAVE_IPS[$i]}" "echo -e '$hosts_content' | sudo tee /etc/hosts > /dev/null" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
  done

  print_success "所有节点hosts文件配置完成"
}

#=============================================================================
# 配置SSH无密码登录
#=============================================================================

configure_ssh_keys() {
  print_step "步骤 4: 配置SSH无密码登录"

  print_info "在所有节点生成SSH密钥并配置互信"

  # 在Master节点生成密钥
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'mkdir -p ~/.ssh && chmod 700 ~/.ssh && ssh-keygen -t rsa -P \"\" -f ~/.ssh/id_rsa -q || true'" "$HADOOP_USER" "HADOOP_PASSWORD"

  # 获取Master的公钥
  local master_pubkey=$(exec_remote "$MASTER_IP" "sudo cat /home/$HADOOP_USER/.ssh/id_rsa.pub" "$HADOOP_USER" "$HADOOP_PASSWORD")

  # 配置Master到自己的无密码登录
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  # 在所有Slave节点配置SSH密钥
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    print_info "配置${SLAVE_HOSTNAMES[$i]}的SSH"

    # 生成Slave的密钥
    exec_remote "${SLAVE_IPS[$i]}" "sudo -u $HADOOP_USER bash -c 'mkdir -p ~/.ssh && chmod 700 ~/.ssh && ssh-keygen -t rsa -P \"\" -f ~/.ssh/id_rsa -q || true'" "$HADOOP_USER" "$HADOOP_PASSWORD"

    # 添加Master的公钥到Slave
    exec_remote "${SLAVE_IPS[$i]}" "sudo -u $HADOOP_USER bash -c 'echo \"$master_pubkey\" >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys'" "$HADOOP_USER" "$HADOOP_PASSWORD"

    # 获取Slave的公钥并添加到Master
    local slave_pubkey=$(exec_remote "${SLAVE_IPS[$i]}" "sudo cat /home/$HADOOP_USER/.ssh/id_rsa.pub" "$HADOOP_USER" "$HADOOP_PASSWORD")
    exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'echo \"$slave_pubkey\" >> ~/.ssh/authorized_keys'" "$HADOOP_USER" "$HADOOP_PASSWORD"
  done

  # 配置SSH客户端，避免首次连接确认
  for node_ip in "$MASTER_IP" "${SLAVE_IPS[@]}"; do
    exec_remote "$node_ip" "sudo -u $HADOOP_USER bash -c 'echo -e \"Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile=/dev/null\" > ~/.ssh/config && chmod 600 ~/.ssh/config'" "$HADOOP_USER" "$HADOOP_PASSWORD"
  done

  print_success "SSH无密码登录配置完成"
}

#=============================================================================
# 安装 Hadoop
#=============================================================================
install_hadoop() {
  print_step "步骤 5: 在 Master 节点下载并配置 Hadoop"

  local local_pkg="$dir/hadoop-${HADOOP_VERSION}.tar.gz"

  #==============================
  # 检查本地是否存在安装包
  #==============================
  if [[ -f "$local_pkg" ]]; then
    print_info "检测到本地已有 Hadoop 安装包：$local_pkg"
    echo "[$MASTER_IP] 是否使用本地包上传安装？(y/N)"
    read -r use_local
    if [[ "$use_local" =~ ^[Yy]$ ]]; then
      print_info "使用本地 Hadoop 安装包进行安装"
      sshpass -p "$HADOOP_PASSWORD" rsync -avz --progress -e "ssh -o StrictHostKeyChecking=no" "$local_pkg" ${HADOOP_USER}@${MASTER_IP}:/tmp/
    else
      print_info "重新下载 Hadoop 包"
      exec_remote "$MASTER_IP" "cd /tmp && wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz || wget -q http://mirrors.cloud.aliyuncs.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" "$HADOOP_USER" "$HADOOP_PASSWORD"
    fi
  else
    print_info "本地未检测到 Hadoop 安装包，开始从镜像源下载..."
    exec_remote "$MASTER_IP" "cd /tmp && wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz || wget -q http://mirrors.cloud.aliyuncs.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" "$HADOOP_USER" "$HADOOP_PASSWORD"
  fi

  #==============================
  # 解压并安装 Hadoop
  #==============================
  print_info "解压并安装 Hadoop"
  exec_remote "$MASTER_IP" "
    cd /tmp &&
    tar -zxf hadoop-${HADOOP_VERSION}.tar.gz &&
    sudo rm -rf /usr/local/hadoop &&
    sudo mv hadoop-${HADOOP_VERSION} /usr/local/hadoop &&
    sudo chown -R $HADOOP_USER:$HADOOP_USER /usr/local/hadoop
  " "$HADOOP_USER" "$HADOOP_PASSWORD"

  #==============================
  # 配置 Hadoop 环境变量
  #==============================
  print_info "配置 Hadoop 环境变量"
  exec_remote "$MASTER_IP" "
    sudo -u $HADOOP_USER bash -c 'grep -q HADOOP_HOME ~/.bashrc || cat >> ~/.bashrc << \"ENVEOF\"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=\\\$PATH:\\\$HADOOP_HOME/bin:\\\$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=\\\$HADOOP_HOME/etc/hadoop
ENVEOF'
  " "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_success "Hadoop 安装完成"
}

#=============================================================================
# 配置Hadoop集群文件
#=============================================================================

configure_hadoop_files() {
  print_step "步骤 6: 配置Hadoop集群文件"

  print_info "配置hadoop-env.sh"
  exec_remote "$MASTER_IP" "sudo sed -i 's|# export JAVA_HOME=|export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64|g' /usr/local/hadoop/etc/hadoop/hadoop-env.sh" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "配置workers文件"
  local workers_content=""
  for hostname in "${SLAVE_HOSTNAMES[@]}"; do
    workers_content+="$hostname\n"
  done
  exec_remote "$MASTER_IP" "echo -e '$workers_content' | sudo tee /usr/local/hadoop/etc/hadoop/workers > /dev/null" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "配置core-site.xml"
  exec_remote "$MASTER_IP" "sudo tee /usr/local/hadoop/etc/hadoop/core-site.xml > /dev/null << 'XMLEOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://${MASTER_HOSTNAME}:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>file:/usr/local/hadoop/tmp</value>
    </property>
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>${HADOOP_USER}</value>
    </property>
</configuration>
XMLEOF
" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "配置hdfs-site.xml"
  exec_remote "$MASTER_IP" "sudo tee /usr/local/hadoop/etc/hadoop/hdfs-site.xml > /dev/null << 'XMLEOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>${SLAVE_COUNT}</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/usr/local/hadoop/tmp/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/usr/local/hadoop/tmp/dfs/data</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>${MASTER_HOSTNAME}:9870</value>
    </property>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>${MASTER_HOSTNAME}:9868</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
</configuration>
XMLEOF
" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "配置yarn-site.xml"
  exec_remote "$MASTER_IP" "sudo tee /usr/local/hadoop/etc/hadoop/yarn-site.xml > /dev/null << 'XMLEOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>${MASTER_HOSTNAME}</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <value>${MASTER_HOSTNAME}:8088</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>
XMLEOF
" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "配置mapred-site.xml"
  exec_remote "$MASTER_IP" "sudo tee /usr/local/hadoop/etc/hadoop/mapred-site.xml > /dev/null << 'XMLEOF'
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.address</name>
        <value>${MASTER_HOSTNAME}:10020</value>
    </property>
    <property>
        <name>mapreduce.jobhistory.webapp.address</name>
        <value>${MASTER_HOSTNAME}:19888</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
XMLEOF
" "$HADOOP_USER" "$HADOOP_PASSWORD"

  # 确保hadoop用户拥有配置文件权限
  exec_remote "$MASTER_IP" "sudo chown -R $HADOOP_USER:$HADOOP_USER /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_success "Hadoop配置文件生成完成"
}

#=============================================================================
# 分发Hadoop到所有Slave节点
#=============================================================================

distribute_hadoop() {
  print_step "步骤 7: 分发Hadoop到所有Slave节点"

  print_info "在Master节点打包Hadoop"
  exec_remote "$MASTER_IP" "cd /usr/local && sudo tar -zcf /tmp/hadoop.tar.gz hadoop && sudo chown $HADOOP_USER:$HADOOP_USER /tmp/hadoop.tar.gz" "$HADOOP_USER" "$HADOOP_PASSWORD"

  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    print_info "分发到${SLAVE_HOSTNAMES[$i]}"

    # 使用hadoop用户通过SSH复制
    exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER scp /tmp/hadoop.tar.gz ${SLAVE_HOSTNAMES[$i]}:/tmp/" "$HADOOP_USER" "$HADOOP_PASSWORD"

    # 在Slave节点解压
    exec_remote "${SLAVE_IPS[$i]}" "cd /tmp && sudo rm -rf /usr/local/hadoop && sudo tar -zxf hadoop.tar.gz -C /usr/local/ && sudo chown -R $HADOOP_USER:$HADOOP_USER /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"

    # 配置Slave节点的环境变量
    exec_remote "${SLAVE_IPS[$i]}" "sudo -u $HADOOP_USER bash -c 'cat >> ~/.bashrc << \"ENVEOF\" "$HADOOP_USER" "$HADOOP_PASSWORD"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=\\\$PATH:\\\$HADOOP_HOME/bin:\\\$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=\\\$HADOOP_HOME/etc/hadoop
ENVEOF
'"
  done

  print_success "Hadoop分发完成"
}

#=============================================================================
# 启动集群
#=============================================================================

start_cluster() {
  print_step "步骤 8: 格式化NameNode并启动集群"

  print_info "格式化NameNode"
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'source ~/.bashrc && hdfs namenode -format -force'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "启动HDFS"
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'source ~/.bashrc && start-dfs.sh'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  sleep 5

  print_info "启动YARN"
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'source ~/.bashrc && start-yarn.sh'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  sleep 5

  print_info "启动JobHistoryServer"
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'source ~/.bashrc && mapred --daemon start historyserver'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  sleep 3

  print_success "集群启动完成"
}

#=============================================================================
# 验证集群
#=============================================================================

verify_cluster() {
  print_step "步骤 9: 验证集群状态"

  print_info "Master节点进程:"
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'source ~/.bashrc && jps'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  echo ""
  print_info "检查HDFS状态:"
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'source ~/.bashrc && hdfs dfsadmin -report'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  echo ""
  print_info "检查YARN节点:"
  exec_remote "$MASTER_IP" "sudo -u $HADOOP_USER bash -c 'source ~/.bashrc && yarn node -list'" "$HADOOP_USER" "$HADOOP_PASSWORD"

  echo -e "\n${GREEN}=== 集群部署完成! ===${NC}"
  echo -e "${YELLOW}Web访问地址:${NC}"
  echo "  - NameNode WebUI: http://${MASTER_IP}:9870"
  echo "  - ResourceManager WebUI: http://${MASTER_IP}:8088"
  echo "  - JobHistory WebUI: http://${MASTER_IP}:19888"
  echo ""
  echo -e "${YELLOW}SSH登录Master节点:${NC}"
  echo "  ssh $HADOOP_USER@$MASTER_IP"
  echo ""
  echo -e "${YELLOW}停止集群命令 (在Master节点以hadoop用户执行):${NC}"
  echo "  mapred --daemon stop historyserver"
  echo "  stop-yarn.sh"
  echo "  stop-dfs.sh"
  echo ""
  echo -e "${YELLOW}测试MapReduce:${NC}"
  echo "  hdfs dfs -mkdir -p /user/hadoop/input"
  echo "  hdfs dfs -put \$HADOOP_HOME/etc/hadoop/*.xml /user/hadoop/input"
  echo "  hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar wordcount /user/hadoop/input /user/hadoop/output"
  echo "  hdfs dfs -cat /user/hadoop/output/part-r-00000"
}

#=============================================================================
# 主函数
#=============================================================================

main() {
  clear
  echo -e "${GREEN}"
  cat <<"EOF"
    ╔═══════════════════════════════════════════════════════════╗
    ║     Hadoop 集群自动部署脚本                               ║
    ║     Hadoop 3.4.2 + Ubuntu 24.04 + OpenJDK 17              ║
    ║     -->密码将明文保存，请仅在虚拟机环境使用！             ║
    ╚═══════════════════════════════════════════════════════════╝
EOF
  echo -e "${NC}"

  # 检查必要工具
  if ! command -v sshpass &>/dev/null; then
    print_info "安装sshpass..."
    sudo apt update && sudo apt install -y sshpass
  fi

  # 执行部署流程
  collect_cluster_info
  configure_all_nodes
  configure_hosts_file
  configure_ssh_keys
  install_hadoop
  configure_hadoop_files
  distribute_hadoop
  start_cluster
  verify_cluster

  print_success "所有步骤执行完成!"
}

# 运行主函数
main "$@"

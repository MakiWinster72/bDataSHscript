#!/bin/bash

#=============================================================================
# Hadoop 集群自动部署脚本 (虚拟机版本) - 修复版
# 适用于: Ubuntu 24.04 + Hadoop 3.4.2 + OpenJDK 17
# 使用方法: bash hadoop_deploy.sh
#=============================================================================

set -e # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
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

print_progress() {
  echo -e "${CYAN}[进度]${NC} $1"
}

# 在远程节点执行命令 - 修复版
exec_remote() {
  local host=$1
  local cmd=$2
  local user=$3
  local pwd=$4

  if [[ "$cmd" == *"sudo"* ]]; then
    sshpass -p "$pwd" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 ${user}@${host} "echo '$pwd' | sudo -S bash -c '$cmd'"
  else
    sshpass -p "$pwd" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 ${user}@${host} "$cmd"
  fi
}

# 向远程节点复制文件
copy_to_remote() {
  local host=$1
  local src=$2
  local dest=$3
  local pwd=$4
  local user=$5
  sshpass -p "$pwd" rsync -avz --progress -e "ssh -o StrictHostKeyChecking=no" "$src" ${user}@${host}:"$dest"
}

# 询问用户选择
ask_user_choice() {
  echo -e "${YELLOW}$prompt${NC}"
  echo "  1) 重新安装 (reinstall)"
  echo "  2) 跳过此步骤 (skip)"
  echo "  3) 退出脚本，手动调试 (exit)"
}

# 显示进度条
show_progress() {
  local current=$1
  local total=$2
  local task=$3
  local percent=$((current * 100 / total))
  local completed=$((current * 50 / total))
  local remaining=$((50 - completed))

  printf "\r${CYAN}[%3d%%]${NC} [" $percent
  printf "%${completed}s" | tr ' ' '='
  printf "%${remaining}s" | tr ' ' '-'
  printf "] %s" "$task"
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
# 检查节点连接性
#=============================================================================

check_node_connectivity() {
  print_step "步骤 2: 检查所有节点连接性"

  local total_nodes=$((SLAVE_COUNT + 1))
  local current=0

  # 检查Master
  current=$((current + 1))
  show_progress $current $total_nodes "检查 Master 节点 ($MASTER_IP)"
  if sshpass -p "$CURRENT_USER_PASSWORD" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 ${CURRENT_USER}@${MASTER_IP} "echo ok" &>/dev/null; then
    echo ""
    print_success "Master 节点连接正常"
  else
    echo ""
    print_error "无法连接到 Master 节点 $MASTER_IP"
    exit 1
  fi

  # 检查所有Slave
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    current=$((current + 1))
    show_progress $current $total_nodes "检查 Slave 节点 ${SLAVE_HOSTNAMES[$i]} (${SLAVE_IPS[$i]})"
    if sshpass -p "$CURRENT_USER_PASSWORD" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 ${CURRENT_USER}@${SLAVE_IPS[$i]} "echo ok" &>/dev/null; then
      echo ""
      print_success "${SLAVE_HOSTNAMES[$i]} 节点连接正常"
    else
      echo ""
      print_error "无法连接到 Slave 节点 ${SLAVE_IPS[$i]}"
      exit 1
    fi
  done

  echo ""
  print_success "所有节点连接检查完成"
}

#=============================================================================
# 配置单个节点
#=============================================================================

configure_single_node() {
  local node_ip=$1
  local hostname=$2
  local is_master=$3
  local node_label="[$hostname]"

  print_info "$node_label 开始基础配置"

  # =====================================================
  # 1. 检查并创建 hadoop 用户
  # =====================================================
  print_progress "$node_label 检查 hadoop 用户..."

  local user_exists=$(sshpass -p "$CURRENT_USER_PASSWORD" ssh -o StrictHostKeyChecking=no ${CURRENT_USER}@${node_ip} \
    "id '$HADOOP_USER' &>/dev/null && echo 'yes' || echo 'no'")

  if [[ "$user_exists" == "yes" ]]; then
    print_success "$node_label hadoop 用户已存在"

    # 先调用函数显示提示信息
    ask_user_choice "$node_label hadoop 用户已存在，是否重新配置?"

    # 读取用户输入到本地变量
    echo -n "请输入选择 [1/2/3] (默认 2): "
    read -r user_input
    user_input=${user_input:-2} # 默认选择 2（跳过）

    # 根据数字设置动作
    case "$user_input" in
    1) choice="reinstall" ;;
    2) choice="skip" ;;
    3) choice="exit" ;;
    *) choice="skip" ;; # 其他输入也默认跳过
    esac

    # 根据 choice 执行操作
    case "$choice" in
    "exit")
      print_warning "用户选择退出"
      exit 0
      ;;
    "reinstall")
      print_info "$node_label 重新配置 hadoop 用户"
      exec_remote "$node_ip" "sudo userdel -r $HADOOP_USER 2>/dev/null || true" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
      exec_remote "$node_ip" "sudo useradd -m -s /bin/bash '$HADOOP_USER' && echo '$HADOOP_USER:$HADOOP_PASSWORD' | sudo chpasswd && sudo usermod -aG sudo '$HADOOP_USER'" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
      ;;
    "skip")
      print_info "$node_label 跳过 hadoop 用户配置"
      ;;
    esac
  else
    print_info "$node_label 创建 hadoop 用户"
    exec_remote "$node_ip" "sudo useradd -m -s /bin/bash '$HADOOP_USER'" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
    exec_remote "$node_ip" "echo '$HADOOP_USER:$HADOOP_PASSWORD' | sudo chpasswd" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
    exec_remote "$node_ip" "sudo usermod -aG sudo '$HADOOP_USER'" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
    print_success "$node_label hadoop 用户创建完成"
  fi

  # =====================================================
  # 2. 设置主机名
  # =====================================================
  print_progress "$node_label 检查主机名..."

  local current_hostname=$(sshpass -p "$CURRENT_USER_PASSWORD" ssh -o StrictHostKeyChecking=no ${CURRENT_USER}@${node_ip} "hostname")

  if [[ "$current_hostname" != "$hostname" ]]; then
    print_info "$node_label 更新主机名: $current_hostname -> $hostname"
    exec_remote "$node_ip" "sudo hostnamectl set-hostname '$hostname'" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
    print_success "$node_label 主机名更新完成"
  else
    print_success "$node_label 主机名已正确: $hostname"
  fi

  # =====================================================
  # 3. 安装基础软件
  # =====================================================
  print_progress "$node_label 检查必要软件..."

  local missing_packages=""
  for pkg in wget ssh sshpass vim net-tools rsync; do
    local installed=$(sshpass -p "$CURRENT_USER_PASSWORD" ssh -o StrictHostKeyChecking=no ${CURRENT_USER}@${node_ip} \
      "dpkg -l | grep -w $pkg &>/dev/null && echo 'yes' || echo 'no'")
    if [[ "$installed" != "yes" ]]; then
      missing_packages="$missing_packages $pkg"
    fi
  done

  if [[ -n "$missing_packages" ]]; then
    print_info "$node_label 安装缺失的软件包:$missing_packages"
    exec_remote "$node_ip" "sudo apt-get update -y" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
    exec_remote "$node_ip" "sudo apt-get install -y $missing_packages" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
    print_success "$node_label 软件包安装完成"
  else
    print_success "$node_label 所有必要软件已安装"
  fi

  # =====================================================
  # 4. 启动 SSH 服务
  # =====================================================
  print_progress "$node_label 配置 SSH 服务..."
  exec_remote "$node_ip" "sudo systemctl enable ssh && sudo systemctl restart ssh" "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
  print_success "$node_label SSH 服务已启动"

  # =====================================================
  # 5. 检测 JDK 安装状态
  # =====================================================
  print_progress "$node_label 检查 JDK 安装状态..."

  local jdk_installed=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${node_ip} \
    "[ -d /usr/lib/jvm/jdk-17.0.12-oracle-x64 ] && java -version 2>&1 | grep -q '17.0.12' && echo 'yes' || echo 'no'")

  if [[ "$jdk_installed" == "yes" ]]; then
    print_success "$node_label JDK 17.0.12 已安装"

    # 调用函数显示提示信息
    ask_user_choice "$node_label JDK已安装，是否重新安装?"

    # 读取用户输入到本地变量
    echo -n "请输入选择 [1/2/3] (默认 2): "
    read -r user_input
    user_input=${user_input:-2} # 默认选择 2（跳过）

    # 数字映射成动作
    case "$user_input" in
    1) choice="reinstall" ;;
    2) choice="skip" ;;
    3) choice="exit" ;;
    *) choice="skip" ;; # 其他输入默认跳过
    esac

    # 执行动作
    case "$choice" in
    "exit")
      print_warning "用户选择退出"
      exit 0
      ;;
    "reinstall")
      print_info "$node_label 重新安装 JDK"
      jdk_installed="no"
      ;;
    "skip")
      print_info "$node_label 跳过 JDK 安装"
      ;;
    esac
  fi

  # =====================================================
  # 6. 安装 JDK
  # =====================================================
  if [[ "$jdk_installed" == "no" ]]; then
    local dir="$(dirname "$0")"
    local jdk_file="$dir/jdk-17.0.12_linux-x64_bin.deb"

    if [[ ! -f "$jdk_file" ]]; then
      print_error "$node_label 未找到 JDK 安装包: $jdk_file"
      print_error "请下载 jdk-17.0.12_linux-x64_bin.deb 并放在脚本同目录下"
      exit 1
    fi

    print_info "$node_label 分发 JDK 安装包..."
    sshpass -p "$HADOOP_PASSWORD" rsync -avz --progress -e "ssh -o StrictHostKeyChecking=no" \
      "$jdk_file" ${HADOOP_USER}@${node_ip}:/tmp/ 2>&1 | grep -v "sending incremental" || true

    print_info "$node_label 安装 JDK..."
    exec_remote "$node_ip" "sudo dpkg -i /tmp/jdk-17.0.12_linux-x64_bin.deb" "$HADOOP_USER" "$HADOOP_PASSWORD"

    print_info "$node_label 配置 JDK 环境变量..."
    exec_remote "$node_ip" "grep -q 'JAVA_HOME.*jdk-17' ~/.bashrc || echo 'export JAVA_HOME=/usr/lib/jvm/jdk-17.0.12-oracle-x64' >> ~/.bashrc" "$HADOOP_USER" "$HADOOP_PASSWORD"
    exec_remote "$node_ip" "grep -q 'PATH.*JAVA_HOME' ~/.bashrc || echo 'export PATH=\$PATH:\$JAVA_HOME/bin' >> ~/.bashrc" "$HADOOP_USER" "$HADOOP_PASSWORD"

    print_success "$node_label JDK 安装完成"
  fi

  # =====================================================
  # 7. 检测 Hadoop 安装状态
  # =====================================================
  print_progress "$node_label 检查 Hadoop 安装状态..."

  local hadoop_installed=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${node_ip} \
    "[ -d /usr/local/hadoop ] && [ -f /usr/local/hadoop/bin/hadoop ] && echo 'yes' || echo 'no'")

  if [[ "$hadoop_installed" == "yes" ]]; then
    print_success "$node_label Hadoop 已安装"

    if [[ "$is_master" != "true" ]]; then
      # 调用函数显示提示信息
      ask_user_choice "$node_label Hadoop已安装，是否重新安装?"

      # 本地读取用户输入
      echo -n "请输入选择 [1/2/3] (默认 2): "
      read -r user_input
      user_input=${user_input:-2} # 默认选择 2（跳过）

      # 数字映射成动作
      case "$user_input" in
      1) choice="reinstall" ;;
      2) choice="skip" ;;
      3) choice="exit" ;;
      *) choice="skip" ;; # 其他输入默认跳过
      esac

      # 执行动作
      case "$choice" in
      "exit")
        print_warning "用户选择退出"
        exit 0
        ;;
      "reinstall")
        print_info "$node_label 将在后续步骤重新安装 Hadoop"
        exec_remote "$node_ip" "sudo rm -rf /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"
        ;;
      "skip")
        print_info "$node_label 跳过 Hadoop 安装"
        ;;
      esac
    fi
  else
    print_info "$node_label Hadoop 未安装，将在后续步骤安装"
  fi

  print_success "$node_label 节点基础配置完成"
}

#=============================================================================
# 所有节点的基础配置
#=============================================================================

configure_all_nodes() {
  print_step "步骤 3: 配置所有节点的基础环境"

  local total_nodes=$((SLAVE_COUNT + 1))
  local current=0

  # 配置 Master 节点
  current=$((current + 1))
  show_progress $current $total_nodes "配置 Master 节点"
  echo ""
  configure_single_node "$MASTER_IP" "$MASTER_HOSTNAME" "true"

  # 配置所有 Slave 节点
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    current=$((current + 1))
    show_progress $current $total_nodes "配置 Slave 节点 ${SLAVE_HOSTNAMES[$i]}"
    echo ""
    configure_single_node "${SLAVE_IPS[$i]}" "${SLAVE_HOSTNAMES[$i]}" "false"
  done

  echo ""
  print_success "所有节点基础配置完成"
}

#=============================================================================
# 配置hosts文件
#=============================================================================

configure_hosts_file() {
  print_step "步骤 4: 配置所有节点的 hosts 文件"

  # 生成hosts内容
  local hosts_content="127.0.0.1 localhost"$'\n'"$MASTER_IP $MASTER_HOSTNAME"
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    hosts_content+=$'\n'"${SLAVE_IPS[$i]} ${SLAVE_HOSTNAMES[$i]}"
  done

  local total_nodes=$((SLAVE_COUNT + 1))
  local current=0

  # 更新 Master 节点
  current=$((current + 1))
  show_progress $current $total_nodes "更新 Master 节点 hosts"
  exec_remote "$MASTER_IP" "
        tmp_file=\$(mktemp)
        echo \"$hosts_content\" > \$tmp_file
        sudo mv \$tmp_file /etc/hosts
        sudo chmod 644 /etc/hosts
    " "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
  echo ""

  # 更新所有 Slave 节点
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    current=$((current + 1))
    show_progress $current $total_nodes "更新 ${SLAVE_HOSTNAMES[$i]} hosts"
    exec_remote "${SLAVE_IPS[$i]}" "
            tmp_file=\$(mktemp)
            echo \"$hosts_content\" > \$tmp_file
            sudo mv \$tmp_file /etc/hosts
            sudo chmod 644 /etc/hosts
        " "$CURRENT_USER" "$CURRENT_USER_PASSWORD"
    echo ""
  done

  print_success "所有节点 hosts 文件配置完成"
}

#=============================================================================
# 配置SSH无密码登录
#=============================================================================

configure_ssh_keys() {
  print_step "步骤 5: 配置 SSH 无密码登录"

  print_info "在 Master 节点生成 SSH 密钥"
  exec_remote "$MASTER_IP" "mkdir -p ~/.ssh && chmod 700 ~/.ssh" "$HADOOP_USER" "$HADOOP_PASSWORD"
  exec_remote "$MASTER_IP" "[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa -q" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "获取 Master 公钥"
  local master_pubkey=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${MASTER_IP} "cat ~/.ssh/id_rsa.pub")

  print_info "配置 Master 到自己的无密码登录"
  exec_remote "$MASTER_IP" "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys" "$HADOOP_USER" "$HADOOP_PASSWORD"

  local total_slaves=$SLAVE_COUNT
  local current=0

  # 配置所有 Slave 节点
  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    current=$((current + 1))
    show_progress $current $total_slaves "配置 ${SLAVE_HOSTNAMES[$i]} SSH"

    exec_remote "${SLAVE_IPS[$i]}" "mkdir -p ~/.ssh && chmod 700 ~/.ssh" "$HADOOP_USER" "$HADOOP_PASSWORD"
    exec_remote "${SLAVE_IPS[$i]}" "[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa -q" "$HADOOP_USER" "$HADOOP_PASSWORD"
    exec_remote "${SLAVE_IPS[$i]}" "echo '$master_pubkey' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys" "$HADOOP_USER" "$HADOOP_PASSWORD"

    local slave_pubkey=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${SLAVE_IPS[$i]} "cat ~/.ssh/id_rsa.pub")
    exec_remote "$MASTER_IP" "echo '$slave_pubkey' >> ~/.ssh/authorized_keys" "$HADOOP_USER" "$HADOOP_PASSWORD"
    echo ""
  done

  # 配置 SSH 客户端
  print_info "配置 SSH 客户端设置"
  for node_ip in "$MASTER_IP" "${SLAVE_IPS[@]}"; do
    exec_remote "$node_ip" "echo -e 'Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile=/dev/null' > ~/.ssh/config && chmod 600 ~/.ssh/config" "$HADOOP_USER" "$HADOOP_PASSWORD"
  done

  print_success "SSH 无密码登录配置完成"
}

#=============================================================================
# 安装 Hadoop
#=============================================================================

install_hadoop() {
  print_step "步骤 6: 在 Master 节点安装 Hadoop"

  # 检查是否已安装
  print_progress "检查 Master 节点 Hadoop 安装状态"
  local hadoop_installed=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${MASTER_IP} \
    "[ -d /usr/local/hadoop ] && [ -f /usr/local/hadoop/bin/hadoop ] && echo 'yes' || echo 'no'")

  if [[ "$hadoop_installed" == "yes" ]]; then
    echo ""

    # 调用函数显示提示信息
    ask_user_choice "Master节点已安装Hadoop，是否重新安装?"

    # 本地读取用户输入
    echo -n "请输入选择 [1/2/3] (默认 2): "
    read -r user_input
    user_input=${user_input:-2} # 默认选择 2（跳过）

    # 数字映射成动作
    case "$user_input" in
    1) choice="reinstall" ;;
    2) choice="skip" ;;
    3) choice="exit" ;;
    *) choice="skip" ;; # 其他输入默认跳过
    esac

    # 执行动作
    case "$choice" in
    "exit")
      print_warning "用户选择退出"
      exit 0
      ;;
    "reinstall")
      print_info "重新安装 Hadoop"
      exec_remote "$MASTER_IP" "sudo rm -rf /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"
      ;;
    "skip")
      print_info "跳过 Hadoop 安装，使用现有版本"
      return 0
      ;;
    esac
  fi

  local dir="$(dirname "$0")"
  local local_pkg="$dir/hadoop-${HADOOP_VERSION}.tar.gz"

  # 检查本地安装包
  if [[ -f "$local_pkg" ]]; then
    print_info "检测到本地 Hadoop 安装包"
    echo -n "是否使用本地安装包? (y/N): "
    read -r use_local
    if [[ "$use_local" =~ ^[Yy]$ ]]; then
      print_info "上传本地 Hadoop 安装包到 Master 节点"
      sshpass -p "$HADOOP_PASSWORD" rsync -avz --progress -e "ssh -o StrictHostKeyChecking=no" \
        "$local_pkg" ${HADOOP_USER}@${MASTER_IP}:/tmp/ 2>&1 | grep -v "sending incremental" || true
    else
      print_info "从镜像源下载 Hadoop"
      exec_remote "$MASTER_IP" "cd /tmp && wget -q --show-progress https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz || wget -q --show-progress http://mirrors.cloud.aliyuncs.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" "$HADOOP_USER" "$HADOOP_PASSWORD"
    fi
  else
    print_info "从镜像源下载 Hadoop"
    exec_remote "$MASTER_IP" "cd /tmp && wget --show-progress https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz || wget --show-progress http://mirrors.cloud.aliyuncs.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" "$HADOOP_USER" "$HADOOP_PASSWORD"
  fi

  print_info "解压并安装 Hadoop"
  exec_remote "$MASTER_IP" "cd /tmp && tar -zxf hadoop-${HADOOP_VERSION}.tar.gz" "$HADOOP_USER" "$HADOOP_PASSWORD"
  exec_remote "$MASTER_IP" "sudo mv /tmp/hadoop-${HADOOP_VERSION} /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"
  exec_remote "$MASTER_IP" "sudo chown -R $HADOOP_USER:$HADOOP_USER /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_info "配置 Hadoop 环境变量"
  exec_remote "$MASTER_IP" "grep -q 'HADOOP_HOME' ~/.bashrc || cat >> ~/.bashrc << 'ENVEOF'
export JAVA_HOME=/usr/lib/jvm/jdk-17.0.12-oracle-x64
export HADOOP_HOME=/usr/local/hadoop
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
ENVEOF" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_success "Hadoop 安装完成"
}

#=============================================================================
# 配置Hadoop集群文件
#=============================================================================

configure_hadoop_files() {
  print_step "步骤 7: 配置 Hadoop 集群文件"

  print_progress "配置 hadoop-env.sh"
  exec_remote "$MASTER_IP" "sed -i 's|# export JAVA_HOME=.*|export JAVA_HOME=/usr/lib/jvm/jdk-17.0.12-oracle-x64|g' /usr/local/hadoop/etc/hadoop/hadoop-env.sh" "$HADOOP_USER" "$HADOOP_PASSWORD"
  echo ""

  print_progress "配置 workers 文件"
  local workers_content=""
  for hostname in "${SLAVE_HOSTNAMES[@]}"; do
    workers_content+="$hostname"$'\n'
  done
  exec_remote "$MASTER_IP" "echo '$workers_content' | tee /usr/local/hadoop/etc/hadoop/workers > /dev/null" "$HADOOP_USER" "$HADOOP_PASSWORD"
  echo ""

  print_progress "配置 core-site.xml"
  exec_remote "$MASTER_IP" "cat > /usr/local/hadoop/etc/hadoop/core-site.xml << 'XMLEOF'
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
  echo ""

  print_progress "配置 hdfs-site.xml"
  exec_remote "$MASTER_IP" "cat > /usr/local/hadoop/etc/hadoop/hdfs-site.xml << 'XMLEOF'
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
  echo ""

  print_progress "配置 yarn-site.xml"
  exec_remote "$MASTER_IP" "cat > /usr/local/hadoop/etc/hadoop/yarn-site.xml << 'XMLEOF'
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
  echo ""

  print_progress "配置 mapred-site.xml"
  exec_remote "$MASTER_IP" "cat > /usr/local/hadoop/etc/hadoop/mapred-site.xml << 'XMLEOF'
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
  echo ""

  exec_remote "$MASTER_IP" "chown -R $HADOOP_USER:$HADOOP_USER /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"

  print_success "Hadoop 配置文件生成完成"
}

#=============================================================================
# 分发Hadoop到所有Slave节点
#=============================================================================

distribute_hadoop() {
  print_step "步骤 8: 分发 Hadoop 到所有 Slave 节点"

  print_info "在 Master 节点打包 Hadoop"
  exec_remote "$MASTER_IP" "cd /usr/local && tar -zcf /tmp/hadoop.tar.gz hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"

  local total_slaves=$SLAVE_COUNT
  local current=0

  for i in $(seq 0 $((SLAVE_COUNT - 1))); do
    current=$((current + 1))
    show_progress $current $total_slaves "分发到 ${SLAVE_HOSTNAMES[$i]}"

    # 检查目标节点是否已有Hadoop
    local slave_has_hadoop=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${SLAVE_IPS[$i]} \
      "[ -d /usr/local/hadoop ] && echo 'yes' || echo 'no'")

    if [[ "$slave_has_hadoop" == "yes" ]]; then
      exec_remote "${SLAVE_IPS[$i]}" "rm -rf /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"
    fi

    # 使用 Master 上的 hadoop 用户直接 scp
    exec_remote "$MASTER_IP" "scp -o StrictHostKeyChecking=no /tmp/hadoop.tar.gz ${SLAVE_HOSTNAMES[$i]}:/tmp/" "$HADOOP_USER" "$HADOOP_PASSWORD"

    # 在 Slave 节点解压
    exec_remote "${SLAVE_IPS[$i]}" "cd /tmp && tar -zxf hadoop.tar.gz && sudo mv hadoop /usr/local/ && sudo chown -R $HADOOP_USER:$HADOOP_USER /usr/local/hadoop" "$HADOOP_USER" "$HADOOP_PASSWORD"

    # 配置环境变量
    exec_remote "${SLAVE_IPS[$i]}" "grep -q 'HADOOP_HOME' ~/.bashrc || cat >> ~/.bashrc << 'ENVEOF'
export JAVA_HOME=/usr/lib/jvm/jdk-17.0.12-oracle-x64
export HADOOP_HOME=/usr/local/hadoop
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
ENVEOF
" "$HADOOP_USER" "$HADOOP_PASSWORD"

    echo ""
  done

  print_success "Hadoop 分发完成"
}

#=============================================================================
# 启动集群
#=============================================================================

start_cluster() {
  print_step "步骤 9: 格式化 NameNode 并启动集群"

  # 检查是否已经格式化
  print_progress "检查 NameNode 是否已格式化"
  local namenode_formatted=$(sshpass -p "$HADOOP_PASSWORD" ssh -o StrictHostKeyChecking=no ${HADOOP_USER}@${MASTER_IP} \
    "[ -d /usr/local/hadoop/tmp/dfs/name/current ] && echo 'yes' || echo 'no'")

  if [[ "$namenode_formatted" == "yes" ]]; then
    echo ""
    print_warning "检测到 NameNode 已经格式化过"

    # 显示提示
    ask_user_choice "NameNode已格式化，是否重新格式化? (重新格式化会清空HDFS数据!)"

    # 本地读取用户输入
    echo -n "请输入选择 [1/2/3] (默认 2): "
    read -r user_input
    user_input=${user_input:-2} # 默认选择 2（跳过）

    # 数字映射成动作
    case "$user_input" in
    1) choice="reinstall" ;;
    2) choice="skip" ;;
    3) choice="exit" ;;
    *) choice="skip" ;;
    esac

    # 执行动作
    case "$choice" in
    "exit")
      print_warning "用户选择退出"
      exit 0
      ;;
    "reinstall")
      print_info "重新格式化 NameNode"
      exec_remote "$MASTER_IP" "rm -rf /usr/local/hadoop/tmp/dfs/name/*" "$HADOOP_USER" "$HADOOP_PASSWORD"
      exec_remote "$MASTER_IP" "source ~/.bashrc && hdfs namenode -format -force" "$HADOOP_USER" "$HADOOP_PASSWORD"
      ;;
    "skip")
      print_info "跳过格式化步骤"
      ;;
    esac
  else
    echo ""
    print_info "格式化 NameNode"
    exec_remote "$MASTER_IP" "source ~/.bashrc && hdfs namenode -format -force" "$HADOOP_USER" "$HADOOP_PASSWORD"
  fi

  print_info "启动 HDFS"
  exec_remote "$MASTER_IP" "source ~/.bashrc && start-dfs.sh" "$HADOOP_USER" "$HADOOP_PASSWORD"
  sleep 5

  print_info "启动 YARN"
  exec_remote "$MASTER_IP" "source ~/.bashrc && start-yarn.sh" "$HADOOP_USER" "$HADOOP_PASSWORD"
  sleep 5

  print_info "启动 JobHistoryServer"
  exec_remote "$MASTER_IP" "source ~/.bashrc && mapred --daemon start historyserver" "$HADOOP_USER" "$HADOOP_PASSWORD"
  sleep 3

  print_success "集群启动完成"
}

#=============================================================================
# 验证集群
#=============================================================================

verify_cluster() {
  print_step "步骤 10: 验证集群状态"

  print_info "检查 Master 节点进程:"
  exec_remote "$MASTER_IP" "source ~/.bashrc && jps" "$HADOOP_USER" "$HADOOP_PASSWORD"

  echo ""
  print_info "检查 HDFS 状态:"
  exec_remote "$MASTER_IP" "source ~/.bashrc && hdfs dfsadmin -report" "$HADOOP_USER" "$HADOOP_PASSWORD"

  echo ""
  print_info "检查 YARN 节点:"
  exec_remote "$MASTER_IP" "source ~/.bashrc && yarn node -list" "$HADOOP_USER" "$HADOOP_PASSWORD"

  echo -e "\n${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
  echo -e "${GREEN}║             集群部署完成!                                  ║${NC}"
  echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}\n"

  echo -e "${YELLOW}📊 Web 访问地址:${NC}"
  echo -e "  ${CYAN}NameNode WebUI:${NC}        http://${MASTER_IP}:9870"
  echo -e "  ${CYAN}ResourceManager WebUI:${NC} http://${MASTER_IP}:8088"
  echo -e "  ${CYAN}JobHistory WebUI:${NC}      http://${MASTER_IP}:19888"
  echo ""

  echo -e "${YELLOW}🔐 SSH 登录 Master 节点:${NC}"
  echo -e "  ${CYAN}ssh $HADOOP_USER@$MASTER_IP${NC}"
  echo ""

  echo -e "${YELLOW}🛑 停止集群命令 (在 Master 节点以 hadoop 用户执行):${NC}"
  echo -e "  ${CYAN}mapred --daemon stop historyserver${NC}"
  echo -e "  ${CYAN}stop-yarn.sh${NC}"
  echo -e "  ${CYAN}stop-dfs.sh${NC}"
  echo ""

  echo -e "${YELLOW}🧪 测试 MapReduce 示例:${NC}"
  echo -e "  ${CYAN}hdfs dfs -mkdir -p /user/hadoop/input${NC}"
  echo -e "  ${CYAN}hdfs dfs -put \$HADOOP_HOME/etc/hadoop/*.xml /user/hadoop/input${NC}"
  echo -e "  ${CYAN}hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar wordcount /user/hadoop/input /user/hadoop/output${NC}"
  echo -e "  ${CYAN}hdfs dfs -cat /user/hadoop/output/part-r-00000${NC}"
  echo ""
}

#=============================================================================
# 主函数
#=============================================================================

main() {
  clear
  echo -e "${GREEN}"
  cat <<"EOF"
    ╔═══════════════════════════════════════════════════════════╗
    ║     Hadoop 集群自动部署脚本 (修复版)                      ║
    ║     Hadoop 3.4.2 + Ubuntu 24.04 + OpenJDK 17              ║
    ║     版本: v2.0 - 完整检测与进度提示                       ║
    ╚═══════════════════════════════════════════════════════════╝
EOF
  echo -e "${NC}"

  # 检查必要工具
  print_info "检查必要工具..."
  if ! command -v sshpass &>/dev/null; then
    print_info "安装 sshpass..."
    sudo apt update && sudo apt install -y sshpass
  fi

  if ! command -v rsync &>/dev/null; then
    print_info "安装 rsync..."
    sudo apt update && sudo apt install -y rsync
  fi

  print_success "必要工具检查完成"

  # 执行部署流程
  collect_cluster_info
  check_node_connectivity
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

# 捕获错误
trap 'print_error "脚本执行出错，请检查错误信息"; exit 1' ERR

# 运行主函数
main "$@"

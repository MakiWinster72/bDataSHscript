#!/bin/bash

# Hadoop 交互式安装配置脚本
# 适用于 Ubuntu/Debian 系统

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
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

# 询问用户是否继续
ask_continue() {
  read -p "$(echo -e ${YELLOW}是否继续? [y/N]: ${NC})" choice
  case "$choice" in
  y | Y | yes | YES) return 0 ;;
  *) return 1 ;;
  esac
}

# 检查是否为 root 用户
check_root() {
  if [ "$EUID" -eq 0 ]; then
    print_error "请不要使用 root 用户运行此脚本"
    exit 1
  fi
}

# 步骤 1: 创建 Hadoop 用户
create_hadoop_user() {
  print_info "========== 步骤 1: 创建 Hadoop 用户 =========="
  echo "当前用户: $(whoami)"
  read -p "是否需要创建新的 hadoop 用户? [y/N]: " create_user

  if [[ $create_user =~ ^[Yy]$ ]]; then
    print_info "创建 hadoop 用户..."
    sudo useradd -m hadoop -s /bin/bash 2>/dev/null || print_warning "用户 hadoop 可能已存在"

    print_info "设置 hadoop 用户密码..."
    sudo passwd hadoop

    print_info "将 hadoop 用户添加到 sudo 组..."
    sudo adduser hadoop sudo

    print_success "Hadoop 用户创建完成"
    print_warning "请将该脚本复制到hadoop用户目录下 ==> sudo cp 脚本路径 /home/hadoop/，\n使用su hadoop切换用户，然后chmod +x hadoopInstall.sh，再./hadoopInstall.sh运行"
    exit 0
  else
    print_info "跳过创建用户步骤"
  fi
}

# 步骤 2: 更新系统并安装基础工具
update_system() {
  print_info "========== 步骤 2: 更新系统并安装基础工具 =========="
  read -p "是否更新 apt 并安装 vim? [y/N]: " update_apt

  if [[ $update_apt =~ ^[Yy]$ ]]; then
    print_info "更新 apt 软件源..."
    sudo apt update

    print_info "安装 vim..."
    sudo apt install -y vim

    print_success "系统更新完成"
  else
    print_info "跳过系统更新"
  fi
}

# 步骤 3: 配置 SSH 无密码登录
configure_ssh() {
  print_info "========== 步骤 3: 配置 SSH 无密码登录 =========="
  read -p "是否配置 SSH 无密码登录(注意当前是否为hadoop用户)? [y/N]: " config_ssh

  if [[ $config_ssh =~ ^[Yy]$ ]]; then
    print_info "安装 openssh-server..."
    sudo apt install -y openssh-server

    print_info "启动 SSH 服务..."
    sudo systemctl start ssh
    sudo systemctl enable ssh

    if [ ! -f ~/.ssh/id_rsa ]; then
      print_info "生成 SSH 密钥..."
      mkdir -p ~/.ssh
      ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa

      print_info "配置无密码登录..."
      cat ~/.ssh/id_rsa.pub >>~/.ssh/authorized_keys
      chmod 600 ~/.ssh/authorized_keys
    else
      print_warning "SSH 密钥已存在，跳过生成"
    fi

    print_success "SSH 配置完成"
    print_info "测试 SSH 连接..."
    ssh -o StrictHostKeyChecking=no localhost "echo 'SSH 无密码登录测试成功!'"
  else
    print_info "跳过 SSH 配置"
  fi
}

# 步骤 4: 安装 Java
install_java() {
  print_info "========== 步骤 4: 安装 Java =========="

  if command -v java &>/dev/null; then
    print_warning "Java 已安装: $(java -version 2>&1 | head -n 1)"
    read -p "是否重新安装? [y/N]: " reinstall_java
    if [[ ! $reinstall_java =~ ^[Yy]$ ]]; then
      print_info "跳过 Java 安装"
      return
    fi
  fi

  print_info "安装 OpenJDK 17..."
  sudo apt install -y openjdk-17-jdk

  print_info "如果阻塞（卡住）请按下Ctrl+C"
  # 查找 Java 安装路径
  JAVA_PATH=$(sudo update-alternatives --config java 2>/dev/null | grep java-17 | awk '{print $3}' | sed 's/\/bin\/java//' | head -n 1)

  if [ -z "$JAVA_PATH" ]; then
    JAVA_PATH="/usr/lib/jvm/java-17-openjdk-amd64"
  fi

  print_info "Java 安装路径: $JAVA_PATH"

  # 配置 JAVA_HOME
  print_info "配置 JAVA_HOME 环境变量..."

  # 检测当前 shell
  if [ -n "$ZSH_VERSION" ]; then
    SHELL_RC="$HOME/.zshrc"
  else
    SHELL_RC="$HOME/.bashrc"
  fi

  # 删除旧的 JAVA_HOME 配置
  sed -i '/export JAVA_HOME.*java.*openjdk/d' "$SHELL_RC"
  sed -i '/export PATH=.*JAVA_HOME/d' "$SHELL_RC"

  # 添加新的配置
  echo "export JAVA_HOME=$JAVA_PATH" >>"$SHELL_RC"
  echo 'export PATH=$PATH:$JAVA_HOME/bin' >>"$SHELL_RC"

  source "$SHELL_RC"
  export JAVA_HOME=$JAVA_PATH
  export PATH=$PATH:$JAVA_HOME/bin

  print_success "Java 安装完成"
  java -version

  # 检测当前 shell 类型
  if [ -n "$ZSH_VERSION" ]; then
    source ~/.zshrc
  else
    source ~/.bashrc
  fi
}

# 步骤 5: 安装 Hadoop
install_hadoop() {
  print_info "========== 步骤 5: 安装 Hadoop =========="

  if [ -d "/usr/local/hadoop" ]; then
    print_warning "检测到 Hadoop 已安装在 /usr/local/hadoop"
    read -p "是否重新安装? [y/N]: " reinstall_hadoop
    if [[ ! $reinstall_hadoop =~ ^[Yy]$ ]]; then
      print_info "跳过 Hadoop 安装"
      return
    fi
    sudo rm -rf /usr/local/hadoop
  fi

  print_info "如果准备了安装包，请确认在~(/home/hadoop/)目录下"

  read -p "请输入 Hadoop 版本 (默认: 3.4.2): " hadoop_version
  hadoop_version=${hadoop_version:-3.4.2}

  HADOOP_FILE="hadoop-$hadoop_version.tar.gz"
  HADOOP_URL="https://mirrors.aliyun.com/apache/hadoop/common/hadoop-$hadoop_version/$HADOOP_FILE"

  print_info "下载 Hadoop $hadoop_version..."
  cd ~

  if [ -f "$HADOOP_FILE" ]; then
    print_warning "安装包已存在，跳过下载"
  else
    wget $HADOOP_URL || {
      print_error "下载失败，请检查网络连接或版本号"
      exit 1
    }
  fi

  print_info "解压 Hadoop..."
  sudo tar -zxf $HADOOP_FILE -C /usr/local
  sudo mv /usr/local/hadoop-$hadoop_version /usr/local/hadoop

  print_info "设置 Hadoop 所有者..."
  sudo chown -R $(whoami):$(whoami) /usr/local/hadoop

  # 配置 Hadoop 环境变量
  print_info "配置 Hadoop 环境变量..."

  if [ -n "$ZSH_VERSION" ]; then
    SHELL_RC="$HOME/.zshrc"
  else
    SHELL_RC="$HOME/.bashrc"
  fi

  sed -i '/export HADOOP_HOME/d' "$SHELL_RC"
  sed -i '/export HADOOP_CONF_DIR/d' "$SHELL_RC"
  sed -i '/export PATH=.*HADOOP_HOME/d' "$SHELL_RC"

  cat >>"$SHELL_RC" <<'EOF'
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
EOF

  source "$SHELL_RC"
  export HADOOP_HOME=/usr/local/hadoop
  export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
  export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

  print_success "Hadoop 安装完成"
  hadoop version
}

# 步骤 6: 配置 Hadoop
configure_hadoop() {
  print_info "========== 步骤 6: 配置 Hadoop =========="
  read -p "是否配置 Hadoop(修改env.sh and core-site and hdfs-site)? [y/N]: " config_hadoop

  if [[ ! $config_hadoop =~ ^[Yy]$ ]]; then
    print_info "跳过 Hadoop 配置"
    return
  fi

  # 配置 hadoop-env.sh
  print_info "配置 hadoop-env.sh..."
  if [ -z "$JAVA_HOME" ]; then
    JAVA_HOME=$(readlink -f $(which java) | sed "s:/bin/java::")
  fi

  sed -i "s|# export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME|g" /usr/local/hadoop/etc/hadoop/hadoop-env.sh

  # 配置 core-site.xml
  print_info "配置 core-site.xml..."
  cat >/usr/local/hadoop/etc/hadoop/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>file:/usr/local/hadoop/tmp</value>
    <description>Abase for other temporary directories.</description>
  </property>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

  # 配置 hdfs-site.xml
  print_info "配置 hdfs-site.xml..."
  cat >/usr/local/hadoop/etc/hadoop/hdfs-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/usr/local/hadoop/tmp/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/usr/local/hadoop/tmp/dfs/data</value>
  </property>
</configuration>
EOF

  print_success "Hadoop 配置完成"
}

# 步骤 7: 格式化 NameNode
format_namenode() {
  print_info "========== 步骤 7: 格式化 NameNode =========="
  read -p "是否格式化 NameNode? (首次安装需要格式化) [y/N]: " format_nn

  if [[ $format_nn =~ ^[Yy]$ ]]; then
    print_warning "格式化 NameNode 会删除所有 HDFS 数据!"
    read -p "确认格式化? [y/N]: " confirm_format

    if [[ $confirm_format =~ ^[Yy]$ ]]; then
      print_info "格式化 NameNode..."
      $HADOOP_HOME/bin/hdfs namenode -format
      print_success "NameNode 格式化完成"
    fi
  else
    print_info "跳过格式化"
  fi
}

# 步骤 8: 启动 HDFS
start_hdfs() {
  print_info "========== 步骤 8: 启动 HDFS =========="
  read -p "是否启动 HDFS? [y/N]: " start_hdfs_choice

  if [[ $start_hdfs_choice =~ ^[Yy]$ ]]; then
    print_info "启动 HDFS..."
    $HADOOP_HOME/sbin/start-dfs.sh

    sleep 5
    print_info "检查 Java 进程..."
    jps

    print_success "HDFS 启动完成"
    print_info "访问 Hadoop Dashboard: http://localhost:9870"
    print_warning "如果是云服务器，请确保安全组开放了 9870 端口"
  else
    print_info "跳过启动 HDFS"
  fi
}

# 步骤 9: 测试 Hadoop
test_hadoop() {
  print_info "========== 步骤 9: 测试 Hadoop =========="
  read -p "是否运行 Hadoop 测试? [y/N]: " test_choice

  if [[ ! $test_choice =~ ^[Yy]$ ]]; then
    print_info "跳过测试"
    return
  fi

  cd $HADOOP_HOME

  print_info "创建 HDFS 用户目录..."
  $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/$(whoami) 2>/dev/null || true

  print_info "创建 input 目录..."
  $HADOOP_HOME/bin/hdfs dfs -rm -r input 2>/dev/null || true
  $HADOOP_HOME/bin/hdfs dfs -mkdir input

  print_info "上传配置文件到 HDFS..."
  $HADOOP_HOME/bin/hdfs dfs -put $HADOOP_HOME/etc/hadoop/*.xml input

  print_info "运行 MapReduce 示例程序 (Grep)..."
  $HADOOP_HOME/bin/hdfs dfs -rm -r output 2>/dev/null || true

  HADOOP_VERSION=$(hadoop version | head -n 1 | awk '{print $2}')
  $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-$HADOOP_VERSION.jar grep input output 'dfs[a-z.]+'

  print_info "查看结果..."
  $HADOOP_HOME/bin/hdfs dfs -cat output/*

  print_success "Hadoop 测试完成"
}

# 主菜单
main_menu() {
  while true; do
    clear
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}   Hadoop 伪分布式安装配置脚本${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "1.  创建 Hadoop 用户"
    echo "2.  更新系统并安装基础工具"
    echo "3.  配置 SSH 无密码登录"
    echo "4.  安装 Java (OpenJDK17)"
    echo "5.  安装 Hadoop (默认3.4.2)"
    echo "6.  配置 Hadoop"
    echo "7.  格式化 NameNode"
    echo "8.  启动 HDFS"
    echo "9.  测试 Hadoop"
    echo "10. 一键完成所有步骤"
    echo "11. 停止 HDFS"
    echo "0.  退出"
    echo ""
    read -p "请选择操作 [0-11]: " choice

    case $choice in
    1) create_hadoop_user ;;
    2) update_system ;;
    3) configure_ssh ;;
    4) install_java ;;
    5) install_hadoop ;;
    6) configure_hadoop ;;
    7) format_namenode ;;
    8) start_hdfs ;;
    9) test_hadoop ;;
    10)
      create_hadoop_user
      update_system
      configure_ssh
      install_java
      install_hadoop
      configure_hadoop
      format_namenode
      start_hdfs
      test_hadoop
      ;;
    11)
      print_info "停止 HDFS..."
      $HADOOP_HOME/sbin/stop-dfs.sh
      print_success "HDFS 已停止"
      ;;
    0)
      print_info "退出脚本"
      exit 0
      ;;
    *)
      print_error "无效选择，请重新输入"
      ;;
    esac

    echo ""
    read -p "按回车键继续..." dummy
  done
}

# 脚本入口
check_root
main_menu

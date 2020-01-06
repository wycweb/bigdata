#一：docker官方文档
https://docs.docker.com/
#二：docker的安装(以centos为例)
操作系统要求  
官方提及的版本为：你需要一个CentOS 7的维护版本  
删除旧版本  

    $ sudo yum remove docker \
                      docker-client \
                      docker-client-latest \
                      docker-common \
                      docker-latest \
                      docker-latest-logrotate \
                      docker-logrotate \
                      docker-engine

***Docker安装***  
1.安装依赖包  

    $ sudo yum install -y yum-utils \
      device-mapper-persistent-data \
      lvm2

2.安装docker yum依赖（安装后文件位置 /etc/yum.repos.d/docker-ce.repo） 

    $ sudo yum-config-manager \
        --add-repo \
        https://download.docker.com/linux/centos/docker-ce.repo


3.安装docker-ce相关依赖  

    $ sudo yum install docker-ce docker-ce-cli containerd.io
    
4.检查docker是否安装成功    
    
    $ yum list docker-ce --showduplicates | sort -r
    
5.启动docker
  
    sudo systemctl start docker  
6.检查状态  

    systemctl status docker
# Docker基础命令
    docker search *** 搜索docker镜像
    docker pull *** 拉取docker镜像
    docker images 查看本地有哪些镜像
    docker run *** 运行指定镜像
    docker exec -it *** 进入到指定镜像
# Docker Hub
    docker 镜像 https://hub.docker.com/
    官方帮助文档 https://docs.docker.com/docker-hub/


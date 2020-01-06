#k8s是什么
官网:<https://kubernetes.io/>
作用:管理容器

#生产主要应用特点
服务和自发现
可伸缩的

#安装部署
地址：<https://kubernetes.io/docs/setup/>  
部署说明：

    Before you begin
    One or more machines running one of:
    Ubuntu 16.04+
    Debian 9+
    CentOS 7
    Red Hat Enterprise Linux (RHEL) 7
    Fedora 25+
    HypriotOS v1.0.1+
    Container Linux (tested with 1800.6.0)
    Swap Disable
    
需要部署的组件:  

    You will install these packages on all of your machines:
    
    (启动进程)kubeadm: the command to bootstrap the cluster.
    
    (服务)kubelet: the component that runs on all of the machines in your cluster and does things like starting pods and containers.
    
    (客户端)kubectl: the command line util to talk to your cluster
    
2.1 最小环境准备  

    bigdata001 docker harbor
    bigdata002 docker 
    bigdata003 docker
    
登录服务(连接harbor)
    
    docker login -u admin -p Harbor123456 bigdata001的ip地址
    
服务器准备工作

        # Set SELinux in disabled mode (effectively disabling it)
        setenforce 0
        sed -i 's/^SELINUX=enforcing$/SELINUX=disabled/' /etc/selinux/config
        
        swapoff -a
        
        cat <<EOF >  /etc/sysctl.d/k8s.conf
        net.bridge.bridge-nf-call-ip6tables = 1
        net.bridge.bridge-nf-call-iptables = 1
        EOF
        sysctl --system
        
2.2 部署yum源安装:  
部署yum源  

    cat <<EOF > /etc/yum.repos.d/kubernetes.repo
    [kubernetes]
    name=Kubernetes
    baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-x86_64
    enabled=1
    gpgcheck=1
    repo_gpgcheck=1
    gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
    EOF
    
    # Set SELinux in permissive mode (effectively disabling it)
    setenforce 0
    sed -i 's/^SELINUX=enforcing$/SELINUX=permissive/' /etc/selinux/config
   
安装：
    
    yum install -y kubelet kubeadm kubectl --disableexcludes=kubernetes
    
    systemctl enable --now kubelet
    
检测：
    
    rpm -qa | grep kube

2.3 三个节点 下载镜像  

    https://hub.docker.com/
    参考：https://hub.docker.com/repository/docker/hackeruncle/pause
    
    docker pull hackeruncle/pause:3.1
    docker tag  hackeruncle/pause:3.1 k8s.gcr.io/pause:3.1
    
    docker pull hackeruncle/etcd:3.2.24
    docker tag hackeruncle/etcd:3.2.24 k8s.gcr.io/etcd:3.2.24
    
    docker pull hackeruncle/coredns:1.2.6
    docker tag hackeruncle/coredns:1.2.6 k8s.gcr.io/coredns:1.2.6
    
    docker pull hackeruncle/kube-scheduler:1.13.2
    docker tag hackeruncle/kube-scheduler:1.13.2 k8s.gcr.io/kube-scheduler:1.13.2
    
    docker pull hackeruncle/kube-controller-manager:1.13.2
    docker tag hackeruncle/kube-controller-manager:1.13.2 k8s.gcr.io/kube-controller-manager:1.13.2
    
    docker pull hackeruncle/kube-proxy:1.13.2
    docker tag hackeruncle/kube-proxy:1.13.2 k8s.gcr.io/kube-proxy:1.13.2
    
    docker pull hackeruncle/kube-apiserver:1.13.2
    docker tag hackeruncle/kube-apiserver:1.13.2 k8s.gcr.io/kube-apiserver:1.13.2

这步并没有推送到harbor，每个节点都应该要操作  


2.4 bigdata001初始化

    kubeadm init \
    --kubernetes-version=v1.13.2 \
    --pod-network-cidr=10.244.0.0/16 \
    --service-cidr=10.96.0.0/12 \
    --ignore-preflight-errors=Swap
    
    mkdir -p $HOME/.kube
    cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
    chown $(id -u):$(id -g) $HOME/.kube/config


创建物理

    [root@bigdata001 ~]# kubectl apply -f kube-flannel.yml
    podsecuritypolicy.policy/psp.flannel.unprivileged created
    clusterrole.rbac.authorization.k8s.io/flannel created
    clusterrolebinding.rbac.authorization.k8s.io/flannel created
    serviceaccount/flannel created
    configmap/kube-flannel-cfg created
    daemonset.apps/kube-flannel-ds-amd64 created
    daemonset.apps/kube-flannel-ds-arm64 created
    daemonset.apps/kube-flannel-ds-arm created
    daemonset.apps/kube-flannel-ds-ppc64le created
    daemonset.apps/kube-flannel-ds-s390x created
    [root@bigdata001 ~]# 

2.5其他节点

    [root@bigdata002 ~]# kubeadm join 172.21.65.102:6443 --token 0u9zvr.g8ca6fb8nbwrcwtn --discovery-token-ca-cert-hash sha256:455e375913ce0e5bfe6710bd71a11b8efcc48b4d60664b502601b7167d1796aa
    [root@bigdata003 ~]# kubeadm join 172.21.65.102:6443 --token 0u9zvr.g8ca6fb8nbwrcwtn --discovery-token-ca-cert-hash sha256:455e375913ce0e5bfe6710bd71a11b8efcc48b4d60664b502601b7167d1796aa
    
    [root@bigdata001 ~]# kubectl get nodes
    NAME           STATUS   ROLES    AGE    VERSION
    bigdata001   Ready    master   7m8s   v1.13.2
    bigdata002   Ready    <none>   35s    v1.13.2
    bigdata003   Ready    <none>   6s     v1.13.2
    [root@bigdata001 ~]# 

主从架构

2.6 Dashboard
2.6.1 安装证书

    [root@bigdata001 ~]# mkdir certs
    [root@bigdata001 ~]# 
    
    
    openssl req -nodes -newkey rsa:2048  \
    -subj "/C=CN/ST=Beijing/L=Beijing/O=example/OU=Personal/CN=kubernetes-dashboard" \
    -keyout dashborad.key \
    -out dashborad.csr
    
    
    openssl  x509 -req -sha256 -days 365 \
    -in dashborad.csr \
    -signkey dashborad.key \
    -out dashborad.crt
    
    [root@bigdata001 certs]# ll
    total 12
    -rw-r--r-- 1 root root 1241 Dec 12 22:40 dashborad.crt
    -rw-r--r-- 1 root root 1021 Dec 12 22:40 dashborad.csr
    -rw-r--r-- 1 root root 1704 Dec 12 22:40 dashborad.key
    [root@bigdata001 certs]# 




启动k8s

    To initialize the control-plane node run:
    
    kubeadm init <args>
    

    

FROM rockylinux:9

RUN dnf install -y openssh-server sudo vim-minimal net-tools procps-ng \
    && dnf clean all \
    && ssh-keygen -A

# Create test users with passwords
RUN useradd -m -s /bin/bash testuser && echo 'test123' | passwd --stdin testuser \
    && useradd -m -s /bin/bash admin && echo 'admin123' | passwd --stdin admin \
    && usermod -aG wheel admin && echo 'admin ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# SSH config: allow password auth
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config \
    && echo 'ClientAliveInterval 60' >> /etc/ssh/sshd_config \
    && echo 'ClientAliveCountMax 3' >> /etc/ssh/sshd_config

# Add some test files and scripts
RUN mkdir -p /opt/app /var/data \
    && printf '#!/bin/bash\necho "App v1.0 running on $(hostname)"\n' > /opt/app/status.sh \
    && chmod +x /opt/app/status.sh \
    && printf 'sample data line 1\nsample data line 2\nsample data line 3\n' > /var/data/test.txt

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D", "-e"]

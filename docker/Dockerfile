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
RUN mkdir -p /opt/app /var/data /opt/backup /tmp/testdir \
    && chmod 777 /opt/backup \
    && printf '#!/bin/bash\necho "App v1.0 running on $(hostname)"\n' > /opt/app/status.sh \
    && chmod +x /opt/app/status.sh \
    && printf 'sample data line 1\nsample data line 2\nsample data line 3\n' > /var/data/test.txt \
    && printf 'config_value=production\n' > /tmp/testdir/app.conf \
    && printf 'old data\n' > /tmp/testdir/overwrite-me.txt

# --- Sample log files for log analyzer testing ---
RUN mkdir -p /opt/ig/logs /opt/am/debug /opt/ds/logs /opt/idm/logs

# PingGateway (IG) log — pipe-delimited format with routeId
COPY test-logs/ig-route.log /opt/ig/logs/route-system.log

# PingAM log — JSON format
COPY test-logs/am-debug.log /opt/am/debug/amAuthentication

# PingDS log — category/severity/msgID format
COPY test-logs/ds-errors.log /opt/ds/logs/errors

# PingIDM log — JSON format (logback JsonEncoder)
COPY test-logs/idm-openidm.log /opt/idm/logs/openidm0.log

# Generic syslog-style log
COPY test-logs/generic-app.log /var/log/app.log

RUN chmod -R 644 /opt/ig/logs/* /opt/am/debug/* /opt/ds/logs/* /opt/idm/logs/* /var/log/app.log

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D", "-e"]

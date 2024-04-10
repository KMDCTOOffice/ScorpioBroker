FROM postgis/postgis

RUN mkdir -p /var/lib/postgresql/
ADD ./ssl/server.crt /var/lib/postgresql/
ADD ./ssl/server.key /var/lib/postgresql/
RUN ls -la /var/lib/postgresql/
RUN chmod 0640 /var/lib/postgresql/server.key /var/lib/postgresql/server.crt
RUN chown root.ssl-cert /var/lib/postgresql/server.key /var/lib/postgresql/server.crt

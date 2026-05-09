FROM nginx:alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY "Link in Bio.html" /usr/share/nginx/html/index.html

EXPOSE 80

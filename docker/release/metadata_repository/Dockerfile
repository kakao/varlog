FROM ***REMOVED***/varlog/dev:latest
ENV DEBIAN_FRONTEND noninteractive

USER root

ADD dist /home/deploy
RUN chown -R deploy:users /home/deploy

USER deploy
WORKDIR /home/deploy

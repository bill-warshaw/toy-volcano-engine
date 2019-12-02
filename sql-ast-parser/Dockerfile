FROM node:10

WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install flora-sql-parser -S
RUN npm install
COPY . .

CMD ["node", "server.js"]
# CMD [ "sleep", "600" ]
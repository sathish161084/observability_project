FROM node:20-alpine AS build
WORKDIR /src
COPY package.json build.js ./
RUN node build.js
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /src/dist /usr/share/nginx/html
EXPOSE 8080
FROM golang:1.21-alpine

WORKDIR /app

COPY go.mod ./

# Generate go.sum and download dependencies
RUN go mod tidy && go mod download

COPY . .

RUN go build -o /shadowcast-signaling .

EXPOSE 8080

CMD ["/shadowcast-signaling"]
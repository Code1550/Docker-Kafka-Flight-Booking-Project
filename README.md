# Docker-Kafka Flight Booking Project

A microservices-based flight booking system built with Docker, Kafka, PostgreSQL, and Redis.

## Architecture

This project implements a distributed flight booking system with the following microservices:

- **API Gateway**: Entry point for all client requests
- **Booking Service**: Manages flight bookings
- **Payment Service**: Handles payment processing
- **Seat Service**: Manages seat availability
- **Notification Service**: Sends notifications

## Prerequisites

- Docker
- Docker Compose
- Python 3.9+

## Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and configure environment variables
3. Run `docker-compose up -d`

## Services

### API Gateway (Port 8000)
- REST API for flight bookings
- Produces booking events to Kafka

### Booking Service
- Consumes booking events
- Stores booking data in PostgreSQL
- Produces booking confirmation events

### Payment Service
- Consumes payment events
- Processes payments
- Produces payment confirmation events

### Seat Service
- Manages seat inventory
- Uses Redis for caching
- Consumes and produces seat availability events

### Notification Service
- Consumes confirmation events
- Sends notifications to customers

## Monitoring

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000

## Development

Each service has its own Dockerfile and requirements.txt for easy development and deployment.

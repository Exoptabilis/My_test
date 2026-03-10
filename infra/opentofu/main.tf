# rarb Infrastructure - AWS EC2 instances
# - us-east-1: Main bot (scanner + executor)
# - ca-central-1: SOCKS5 proxy for order placement

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
  }
}

# Cloudflare provider (using Global API Key)
provider "cloudflare" {
  api_key = var.cloudflare_api_key
  email   = var.cloudflare_email
}

# Provider for us-east-1 (bot server)
provider "aws" {
  region = "us-east-1"
  alias  = "us_east"
}

# Provider for ca-central-1 (proxy server)
provider "aws" {
  region = "ca-central-1"
  alias  = "ca_central"
}

# VPC for us-east-1 (no default VPC exists)
resource "aws_vpc" "bot_vpc" {
  provider             = aws.us_east
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name    = "rarb-vpc"
    Project = "rarb"
  }
}

# Internet Gateway for us-east-1
resource "aws_internet_gateway" "bot_igw" {
  provider = aws.us_east
  vpc_id   = aws_vpc.bot_vpc.id

  tags = {
    Name    = "rarb-igw"
    Project = "rarb"
  }
}

# Public Subnet for us-east-1
resource "aws_subnet" "bot_subnet" {
  provider                = aws.us_east
  vpc_id                  = aws_vpc.bot_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name    = "rarb-subnet"
    Project = "rarb"
  }
}

# Route Table for us-east-1
resource "aws_route_table" "bot_rt" {
  provider = aws.us_east
  vpc_id   = aws_vpc.bot_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bot_igw.id
  }

  tags = {
    Name    = "rarb-rt"
    Project = "rarb"
  }
}

# Route Table Association
resource "aws_route_table_association" "bot_rta" {
  provider       = aws.us_east
  subnet_id      = aws_subnet.bot_subnet.id
  route_table_id = aws_route_table.bot_rt.id
}

# SSH Key Pair - us-east-1
resource "aws_key_pair" "bot_key" {
  provider   = aws.us_east
  key_name   = "rarb-bot-key"
  public_key = var.ssh_public_key
}

# SSH Key Pair - ca-central-1
resource "aws_key_pair" "proxy_key" {
  provider   = aws.ca_central
  key_name   = "rarb-proxy-key"
  public_key = var.ssh_public_key
}

# Security Group for Bot Server (us-east-1)
resource "aws_security_group" "bot_sg" {
  provider    = aws.us_east
  name        = "rarb-bot-sg"
  description = "Security group for rarb bot server"
  vpc_id      = aws_vpc.bot_vpc.id

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_allowed_cidrs
    description = "SSH access"
  }

  # Dashboard (HTTP - for LetsEncrypt challenge)
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP for LetsEncrypt"
  }

  # Dashboard (HTTPS)
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Dashboard HTTPS"
  }

  # All outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "rarb-bot-sg"
    Project = "rarb"
  }
}

# Security Group for Proxy Server (ca-central-1)
resource "aws_security_group" "proxy_sg" {
  provider    = aws.ca_central
  name        = "rarb-proxy-sg"
  description = "Security group for rarb SOCKS5 proxy"

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_allowed_cidrs
    description = "SSH access"
  }

  # SOCKS5 proxy - only from bot server
  ingress {
    from_port   = 1080
    to_port     = 1080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Will be restricted after bot IP is known
    description = "SOCKS5 proxy access"
  }

  # All outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "rarb-proxy-sg"
    Project = "rarb"
  }
}

# Get latest Ubuntu 24.04 AMI - us-east-1
data "aws_ami" "ubuntu_us_east" {
  provider    = aws.us_east
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Get latest Ubuntu 24.04 AMI - ca-central-1
data "aws_ami" "ubuntu_ca_central" {
  provider    = aws.ca_central
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Bot Server (us-east-1)
resource "aws_instance" "bot" {
  provider               = aws.us_east
  ami                    = data.aws_ami.ubuntu_us_east.id
  instance_type          = var.bot_instance_type
  key_name               = aws_key_pair.bot_key.key_name
  subnet_id              = aws_subnet.bot_subnet.id
  vpc_security_group_ids = [aws_security_group.bot_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  tags = {
    Name    = "rarb-bot"
    Project = "rarb"
    Role    = "bot"
  }
}

# Proxy Server (ca-central-1)
resource "aws_instance" "proxy" {
  provider               = aws.ca_central
  ami                    = data.aws_ami.ubuntu_ca_central.id
  instance_type          = var.proxy_instance_type
  key_name               = aws_key_pair.proxy_key.key_name
  vpc_security_group_ids = [aws_security_group.proxy_sg.id]

  root_block_device {
    volume_size = 8
    volume_type = "gp3"
  }

  tags = {
    Name    = "rarb-proxy"
    Project = "rarb"
    Role    = "proxy"
  }
}

# Update proxy security group to only allow bot IP
resource "aws_security_group_rule" "proxy_from_bot" {
  provider          = aws.ca_central
  type              = "ingress"
  from_port         = 1080
  to_port           = 1080
  protocol          = "tcp"
  cidr_blocks       = ["${aws_instance.bot.public_ip}/32"]
  security_group_id = aws_security_group.proxy_sg.id
  description       = "SOCKS5 from bot server only"

  # This replaces the open 0.0.0.0/0 rule after bot IP is known
  depends_on = [aws_instance.bot]
}

# Cloudflare DNS - point rarb.arkets.com to bot server
data "cloudflare_zone" "arkets" {
  name = "arkets.com"
}

resource "cloudflare_record" "rarb" {
  zone_id = data.cloudflare_zone.arkets.id
  name    = "rarb"
  content = aws_instance.bot.public_ip
  type    = "A"
  ttl     = 60  # Low TTL for easy updates
  proxied = false  # Direct connection, no Cloudflare proxy (for WebSocket compatibility)
}

resource "cloudflare_record" "rarb_www" {
  zone_id = data.cloudflare_zone.arkets.id
  name    = "www.rarb"
  content = "rarb.arkets.com"
  type    = "CNAME"
  ttl     = 300
  proxied = false
}

# Cloudflare DNS - point rarb-proxy.arkets.com to proxy server (Montreal)
resource "cloudflare_record" "rarb_proxy" {
  zone_id = data.cloudflare_zone.arkets.id
  name    = "rarb-proxy"
  content = aws_instance.proxy.public_ip
  type    = "A"
  ttl     = 60
  proxied = false
}

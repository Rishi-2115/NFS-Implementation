# Network File System (NFS) Implementation

A distributed file system implementation in C that allows multiple clients to access files stored on remote storage servers through a centralized naming server.

## Overview

This project implements a simplified version of a Network File System with the following components:

- **Naming Server**: Central coordinator that manages file metadata and routes client requests
- **Storage Server**: Stores actual file data and handles file operations
- **Client**: Interface for users to perform file operations on the distributed system

## Architecture
Client <---> Naming Server <---> Storage Server


The naming server acts as a mediator between clients and storage servers, maintaining a directory structure and file location information.

## Features

- **File Operations**: Create, read, write, delete files
- **Directory Operations**: Create and list directories
- **Distributed Storage**: Files distributed across multiple storage servers
- **Concurrent Access**: Multiple clients can access the system simultaneously
- **Fault Tolerance**: Basic error handling and recovery mechanisms

## Files Structure
├── FinalSubmission/ # First implementation
│ ├── client.c
│ ├── naming_server.c
│ └── storage_server.c
├── FinalSubmission2/ # Updated implementation
│ ├── client.c
│ ├── naming_server.c
│ └── storage_server.c
└── README.md


## Compilation

Compile each component separately:

```bash
# Compile naming server
gcc -o naming_server naming_server.c

# Compile storage server
gcc -o storage_server storage_server.c

# Compile client
gcc -o client client.c
Usage
1. Start the Naming Server
./naming_server
2. Start Storage Server(s)
./storage_server
3. Run Client
./client
Supported Operations
CREATE <filename> - Create a new file
READ <filename> - Read file contents
WRITE <filename> <data> - Write data to file
DELETE <filename> - Delete a file
LIST - List all files in the system
MKDIR <dirname> - Create a directory
RMDIR <dirname> - Remove a directory
Implementation Details
Naming Server
Maintains file system metadata
Routes client requests to appropriate storage servers
Handles directory structure and permissions
Storage Server
Stores actual file data
Processes read/write operations
Manages local file system operations
Client
Provides user interface for file operations
Communicates with naming server via sockets
Handles user input and displays results
Network Communication
The system uses TCP sockets for communication between components:

Client ↔ Naming Server
Naming Server ↔ Storage Server
Error Handling
Connection failures
File not found errors
Permission denied errors
Storage server unavailability
Future Enhancements
Load balancing across multiple storage servers
File replication for redundancy
Advanced caching mechanisms
User authentication and authorization
File locking mechanisms
Requirements
GCC compiler
Linux/Unix environment
TCP/IP networking support
Contributing
Fork the repository
Create a feature branch
Make your changes
Submit a pull request
License
This project is for educational purposes.


This README provides a comprehensive overview of your NFS project, including architecture, features, usage instructions, and implementation details.

can u add this directly to github repo

The user wants me to add the README content directly to the GitHub repository. I need to:

Update the README.md file with the content I provided
Add and commit the changes to git
Push to GitHub (though they'll need to set up the remote first)
Let me update the README file first.


NFS-main
/
README.md


NFS-main
/
README.md

+139

@@ -1,0 +1,138 @@
# Network File System (NFS) Implementation

A distributed file system implementation in C that allows multiple clients to access files stored on remote storage servers through a centralized naming server.

## Overview

This project implements a simplified version of a Network File System with the following components:

- **Naming Server**: Central coordinator that manages file metadata and routes client requests
- **Storage Server**: Stores actual file data and handles file operations
- **Client**: Interface for users to perform file operations on the distributed system

## Architecture

```
Client <---> Naming Server <---> Storage Server
```

The naming server acts as a mediator between clients and storage servers, maintaining a directory structure and file location information.

## Features

- **File Operations**: Create, read, write, delete files
- **Directory Operations**: Create and list directories
- **Distributed Storage**: Files distributed across multiple storage servers
- **Concurrent Access**: Multiple clients can access the system simultaneously
- **Fault Tolerance**: Basic error handling and recovery mechanisms

## Files Structure

```
├── FinalSubmission/        # First implementation
│   ├── client.c
│   ├── naming_server.c
│   └── storage_server.c
├── FinalSubmission2/       # Updated implementation
│   ├── client.c
│   ├── naming_server.c
│   └── storage_server.c
└── README.md
```

## Compilation

Compile each component separately:

```bash
# Compile naming server
gcc -o naming_server naming_server.c

# Compile storage server
gcc -o storage_server storage_server.c

# Compile client
gcc -o client client.c
```

## Usage

### 1. Start the Naming Server
```bash
./naming_server
```

### 2. Start Storage Server(s)
```bash
./storage_server
```

### 3. Run Client
```bash
./client
```

## Supported Operations

- `CREATE <filename>` - Create a new file
- `READ <filename>` - Read file contents
- `WRITE <filename> <data>` - Write data to file
- `DELETE <filename>` - Delete a file
- `LIST` - List all files in the system
- `MKDIR <dirname>` - Create a directory
- `RMDIR <dirname>` - Remove a directory

## Implementation Details

### Naming Server
- Maintains file system metadata
- Routes client requests to appropriate storage servers
- Handles directory structure and permissions

### Storage Server
- Stores actual file data
- Processes read/write operations
- Manages local file system operations

### Client
- Provides user interface for file operations
- Communicates with naming server via sockets
- Handles user input and displays results

## Network Communication

The system uses TCP sockets for communication between components:
- Client ↔ Naming Server
- Naming Server ↔ Storage Server

## Error Handling

- Connection failures
- File not found errors
- Permission denied errors
- Storage server unavailability

## Future Enhancements

- Load balancing across multiple storage servers
- File replication for redundancy
- Advanced caching mechanisms
- User authentication and authorization
- File locking mechanisms

## Requirements

- GCC compiler
- Linux/Unix environment
- TCP/IP networking support

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is for educational purposes.

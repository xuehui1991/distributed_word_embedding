#ifndef DISTRIBUTED_WORD_EMBEDDING_CLIENT_H_
#define DISTRIBUTED_WORD_EMBEDDING_CLIENT_H_

#include <stdio.h>
#include <sys/types.h>
#include <windows.h>
#include <process.h>
#include <iostream>
#include <json/json.h>
#include <thread>
#pragma comment(lib,"ws2_32.lib")



namespace wordembedding
{
  class Client
  {
  public:
    Client(std::vector<char*> server_point, std::vector<int> server_port, int client_size, std::string identity, int local_port);
    void Start();

    std::string MakeJson();
    void ParseJson(int* rank, char** endpoint);

    int  GetResultSize();

  private:
    std::vector<char*> server_point_;
    std::vector<int> server_port_;
    int local_port_;

    int client_size_;
    std::string identity_;
    std::vector<std::string> result_;

    unsigned long GetServerIP(char* end_point);

    void Connect(SOCKET &sock, char* end_point, int port);

    void SendMessages(SOCKET &sock, std::string message);

    void ReceiveMessages(PVOID param);
  };
}

#endif
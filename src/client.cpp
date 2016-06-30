#include "client.h"
#include <unordered_set>

namespace multiverso
{
	namespace wordembedding
	{
		Client::Client(std::vector<char*> server_point, std::vector<int> server_port, int client_size, std::string identity,int local_port)
		{
			server_point_ = server_point;
			server_port_ = server_port;
			client_size_ = client_size;
			identity_ = identity;
			local_port_ = local_port;
		}

		int Client::GetResultSize()
		{
			return result_.size();
		}

		void Client::Start()
		{
			WSADATA wsa;
			if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0){
				std::cout << "init socket resource fail..." << std::endl;
				Sleep(3000);
				exit(-1);
			}

			std::vector<SOCKET> socket_list;
			std::vector<std::thread> thread_pool;
			int broker_number = server_point_.size();
			for (int i = 0; i < broker_number; i++)
			{
				SOCKET sock;
				struct timeval timeout;
				timeout.tv_sec = 10000000;
				timeout.tv_usec = 0;

				std::cout << "broker number" << broker_number << std::endl;

				if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == INVALID_SOCKET){
					std::cout << "create socket fail..." << std::endl;
					exit(-1);
				}

				if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout,
					sizeof(timeout)) < 0)
					std::cout << "set socket rceive option failed..." << std::endl;

				if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout,
					sizeof(timeout)) < 0)
					std::cout << "set socket send option failed..." << std::endl;				

				socket_list.push_back(sock);
				Connect(socket_list[i], server_point_[i], server_port_[i]);
				std::string message = MakeJson();
				std::cout << message << std::endl;
				SendMessages(socket_list[i], message);
				thread_pool.push_back(std::thread(&Client::ReceiveMessages,
					this, &socket_list[i]));
				thread_pool[i].join();
			}

			std::cout << "result size = " << result_.size() << std::endl;
			std::cout << "server_point_ = " << server_point_.size() << std::endl;

			while (result_.size()< broker_number)
			{
				Sleep(3000);
				std::cout << "result size = " << result_.size() <<std::endl;
			}

			for (int i = 0; i < broker_number; i++)
			{
				closesocket(socket_list[i]);
			}
			
			std::cout << "close the socket."<< std::endl;
			WSACleanup();
		}

		void  Client::ReceiveMessages(PVOID param)
		{
			char buf[5000];

			SOCKET* sock = (SOCKET*)param;
			int bytes;
			if ((bytes = recv(*sock, buf, sizeof(buf), 0)) == SOCKET_ERROR){
				std::cout << "receive fail...." << std::endl;
				exit(-1);
			}
			buf[bytes] = '\0';
			std::cout << "receive message from server" << buf << std::endl;
			std::string message(buf);
			result_.push_back(message);
		}


		void Client::SendMessages(SOCKET &sock, std::string message)
		{
			const char* buf = message.c_str();
			if (send(sock, buf, strlen(buf), 0) == SOCKET_ERROR){
				std::cout << "send message fail..." << std::endl;
				exit(-1);
			}
		}

		unsigned long Client::GetServerIP(char* end_point)
		{
			unsigned long ip;
			if ((ip = inet_addr(end_point)) == INADDR_NONE){
				std::cout << "this ip address didn't exit." << std::endl;
				Sleep(3000);
				exit(-1);
			}
			return ip;
		}

		void Client::Connect(SOCKET &sock, char* end_point, int port)
		{
			unsigned long ip = GetServerIP(end_point);
			std::cout << "Connecting to " << inet_ntoa(*(in_addr*)&ip) << " : " << port << std::endl;
			struct sockaddr_in serverAddress;
			memset(&serverAddress, 0, sizeof(sockaddr_in));
			serverAddress.sin_family = AF_INET;
			serverAddress.sin_addr.S_un.S_addr = ip;
			serverAddress.sin_port = htons(port);

			if (connect(sock, (sockaddr*)&serverAddress, sizeof(serverAddress)) == SOCKET_ERROR){
				std::cout << "connecting fail..." << WSAGetLastError() << std::endl;
				Sleep(3000);
				exit(-1);
			}
		}

		std::string Client::MakeJson()
		{
			char buf[5000];
			sprintf(buf, "{\"clientsSize\":%d,\"identity\":%s,\"port\":%d}", client_size_, identity_.c_str(), local_port_);
			return buf;
		}

		void Client::ParseJson(int* rank, char** endpoint)
		{
			std::unordered_set<std::string> endpoint_set;

			Json::Reader reader;
			int count = 0;
			for (int i = 0; i < result_.size(); i++)
			{
				Json::Value root;
				std::string json = result_[i];
				if (reader.parse(json, root))
				{
					Json::Value linkinfos = root["linkinfos"];
					int ClientIps_size = linkinfos["ClientIps"].size();

					for (int j = 0; j < ClientIps_size; ++j)
					{
						char port[100];
						char *point_port = new(std::nothrow)char[300];

						const char * point = linkinfos["ClientIps"][j].asCString();
						int port_number = linkinfos["ClientPorts"][j].asInt();
						strcpy(point_port, point);
						itoa(port_number, port, 10);
						strcat(point_port, ":");
						strcat(point_port, port);
						std::string string_point(point_port);
						std::cout << "point " << string_point<< std::endl;

						endpoint_set.find(string_point);
						if (endpoint_set.find(string_point) == endpoint_set.end())
						{
							rank[count] = count;
							endpoint[count] = point_port;
							std::cout << "rank " << rank[count] << std::endl;
							std::cout << "endpoint " << endpoint[count] << std::endl;
							endpoint_set.insert(string_point);
							count++;
						}

	
					}
				}
			}
		}
	}
}

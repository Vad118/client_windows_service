//Client

#include <iostream>
#include <stdlib.h>
#include <winsock2.h>
#include <process.h>
#include <string>
#include <fstream>
#include <map>
#include <iterator>
#include <cctype>
#include "worker.h"
extern "C"
{
    #include <lua.h>
    #include <lualib.h>
    #include <lauxlib.h>
}
#pragma comment(lib, "lua51.lib")
#pragma comment(lib, "Ws2_32.lib")

using namespace std;

enum{STR_SIZE=256,PORT=7500,PORT_MONITORING=7501, ARBITERS_MAX=100};

struct actor
{
    string behavior;
    char parameters[5][50];
    lua_State *luaVM;
    int count;
};

struct saveActor
{
     char behavior[100];
     char parameters[5][50];
     int count;
     char id[10];

     int totalSaveCount;
     int totalUnreadMessages;
};

struct dispatcher_answer
{
    int command;   //�������, ���������������� �� ������� �������.
    // 1 - create
    // 2 - send
    // 4 - print
    // 5 - finish print
    // 6 - �������������� �������� �������(���� ����������)
    // 61- ���� ������� ��������� �������
    // 62- ������� ��������� ��������
    // 7 - ������ �������(������ ���������)
    // 8 - ������ ���������� ����� ��������
    // 9 - ����������
    // 10 - ������ ����������
    int worker_id; //���� ����������
    char arbiter_id[STR_SIZE]; //������, �������� ����������
    char arbiter_parent[10]; // ������, ������� ����������

    //��������� actor
    char actor_behavior[STR_SIZE];
    char actor_parameters[5][50]; //�������� ��� �������� � send
    int actor_par_count;
    char script[1024];
};

// ��������� �������� ������������ �������
struct sendStruct
{
    // 0 - ������� �����
    // 1 - ����������
    // 2 - ����
    // 3 - ��������� ���
    // 4 - ����������
    int command;
};

// ��������� ���������� �� ������� ��� �����������/�����������/����������
struct receiveStruct
{
    // 0 - create
    // 1 - send
    // 2 - become
    // 31 - save_start_send
    // 32 - save_end_send
    int command;
    char text[STR_SIZE];
    char arbiter_id[STR_SIZE];
    char arbiter_parent[10];
};

///////////////////////

int currentState=0; // ������������� sendStruct � ����������� -1 - ���������, �������� �� ��������������
int previousState=0;
bool self_setted=false;
bool global_quit=false;


map <string,actor> actors;
char scriptFileName[STR_SIZE];
lua_State *g_LuaVM = NULL; //����������, ���������� ��� CreateAndInitActors, � ���� ��������� ������� ���� ���������� ��������������


int send_actor_obr(dispatcher_answer answer);
void sendAnswer(int command, char arbiter_id[STR_SIZE], actor act, char arbiter_parent[STR_SIZE]);
bool init_lua(char *scriptFileName, lua_State *&loc_luaVM);
void start_lua(char *func_name);
bool LoadScript(char *script);
void _lua_pushStringOrNumber(lua_State *g_LuaVM, const char *str);
void get_actor_func_name(char &func_name, bool &spec_func_name, lua_State *luaVM, const char *str, char send_parameters[5][50]);

void stopWaitForContinue(); // ������� ��������������� ���������� ��������� � ��������� �������.
void save();
void clear();
///////////////////////
class _client
{
private:
    char p_adr[30];
    int domain;
    int type;
    int protocol;
public:
    char client_id[STR_SIZE];  // �������� �� �������, ��� ������������ id ��������
    SOCKET my_sock,monitoring_sock;
    int arbiter_num; // ������ �������, ������������� ��� �������� �������. ��� �������: "client_id;arbiter_num"
    _client(char _p_adr[30]);
    bool connectToServer();
    bool connectToServerSecondSocket(string client_id);
    //bool mainCycle();
    bool readInput();
    char *getCin();
    char *generateArbiterId();
}*client;

_client::_client(char _p_adr[30])
{
    strcpy(p_adr,_p_adr);
    arbiter_num=0;
}
bool _client::connectToServer()
{
    int domain=AF_INET; //�������� ����� Internet
    int type=SOCK_STREAM; //������������ ������� ���������� �������� �� ������ ������������ ����������� ����������
    int protocol=0;//� ��������� TCP/IP �������� ���� ������������ ����� ������, ������� � ���-�� �������� 0
    char buf[STR_SIZE]; //������ ��������� ������
    char srv_resp[STR_SIZE];//����������� ��������� �������
    //��������� ��� �����������
    struct sockaddr_in peer;
    peer.sin_family=domain;
    peer.sin_port=htons(PORT);    //����� �����
    peer.sin_addr.s_addr=inet_addr(p_adr); //IP ����� �������

    //��� select
    //+++++
    bool error_fl=false;
    WSADATA WsaData;
    int err = WSAStartup (0x0101, &WsaData);
    if (err == SOCKET_ERROR)
    {
        cout<<"#ERROR with WSAStrartup()";
        error_fl=true;
    }
    my_sock=socket(domain, type, protocol);

    if(my_sock<0)
    {
        cout<<"#ERROR with socket";
        error_fl=true;
    }

    if(connect(my_sock,(struct sockaddr *)&peer,sizeof(peer))<0) //����������� �� ������
    {
        cout<<"#ERROR with connect";
        error_fl=true;
    }
    //���������� �� ������ nick
    send(my_sock," ",1,0);
    //�������� �� ������� ��������� � ������������� �����������
    recv(my_sock,srv_resp,STR_SIZE,0);
    bool quit=true;
    if(!error_fl && strcmp(srv_resp,"")!=0)
    {
        strcpy(client_id,srv_resp);
        // ����������� �� ������� ������
        if(connectToServerSecondSocket(client_id))
        {
            cout<<"Connection Established."<<endl<<"Client ID:"<<client_id<<endl;
            quit=false;
        }
    }
    return quit;
}

bool _client::connectToServerSecondSocket(string client_id)
{
    int domain=AF_INET; //�������� ����� Internet
    int type=SOCK_STREAM; //������������ ������� ���������� �������� �� ������ ������������ ����������� ����������
    int protocol=0;//� ��������� TCP/IP �������� ���� ������������ ����� ������, ������� � ���-�� �������� 0
    char buf[STR_SIZE]; //������ ��������� ������
    char srv_resp[STR_SIZE];//����������� ��������� �������
    //��������� ��� �����������
    struct sockaddr_in peer;
    peer.sin_family=domain;
    peer.sin_port=htons(PORT_MONITORING);    //����� �����
    peer.sin_addr.s_addr=inet_addr(p_adr); //IP ����� �������

    //��� select
    //+++++
    bool error_fl=false;
    WSADATA WsaData;
    int err = WSAStartup (0x0101, &WsaData);
    if (err == SOCKET_ERROR)
    {
        cout<<"#ERROR with WSAStrartup()";
        error_fl=true;
    }
    monitoring_sock=socket(domain, type, protocol);

    if(monitoring_sock<0)
    {
        cout<<"#ERROR with socket MONITORING";
        error_fl=true;
    }

    if(connect(monitoring_sock,(struct sockaddr *)&peer,sizeof(peer))<0) //����������� �� ������
    {
        cout<<"#ERROR with connect to MONITORING";
        error_fl=true;
    }
    //���������� �� ������ nick
    send(monitoring_sock,client_id.c_str(),STR_SIZE,0);
    //�������� �� ������� ��������� � ������������� �����������
    recv(monitoring_sock,srv_resp,STR_SIZE,0);
    bool good=false;
    if(!error_fl && strcmp(srv_resp,"done")==0)
    {
        //cout<<"Connection Established."<<endl;
        good=true;
    }
    return good;
}

bool _client::readInput()
{
   char *part_str;
   char buf[STR_SIZE]; //������ ��������� ������
   struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
   fd_set readfds;
   bool quit=false;

   strcpy(buf,getCin());

   part_str=strtok(buf," ");//�������� ������ ����� �� �������. ��� ��������
   //��������� ������ �������� + ������ ������� - ��� ��������
   if(part_str!=0)
   {
       /*int length_com=strlen(part_str);
       if(strcmp(part_str,"SEND")==0) //������� �� ����� ���������
       {
            //������� ������ 5 �������� "SEND " � ���������� � buf ���������
            part_str=strtok(NULL," ");
            char buf2[STR_SIZE]="";
            while(part_str!=NULL)
            {
                strcat(buf2,part_str);
                strcat(buf2," ");     //�����, �.�. ������ 1 ������ ����� �������� ����� �������
                part_str=strtok(NULL," ");
            }
            //���������� ��������� �� ������
            send(my_sock,buf2,STR_SIZE,0);
       }
       else */
       if(strcmp(part_str,"QUIT")==0)
           quit=true;
   }
   return quit;
}

/*void sendSocket(SOCKET my_sock, dispatcher_answer *answer) //������ ������� ��������� ����� �����
{
   char buf[STR_SIZE]; //������ ��������� ������
   strcpy(buf,input);
   //���������� ��������� �� ������
   send(my_sock,buf,STR_SIZE,0);
}*/

void recv_file(SOCKET my_sock,int worker_id)
{
    bool finish=false;
    bool started=false;
    char buf[1024];
    dispatcher_answer answer;
    char *pBuff = new char[sizeof(dispatcher_answer)];
    char addr[STR_SIZE];
	strcpy(addr,FILE_ADDR);
    char fname[STR_SIZE];
    _itoa(worker_id,fname,10);
    strcat(addr,fname);
    strcat(addr,".lua");
    strcpy(scriptFileName,addr);
    FILE *in = fopen(addr, "w");
    while (!finish)
    {
      int nbytes = recv( my_sock, pBuff, sizeof(answer), 0 );
      memcpy(&answer,pBuff,sizeof(dispatcher_answer));
      if ( nbytes == 0)
      {
        cout<<"Disconnected."<<endl;
        return;
      }
      if (nbytes < 0)
      {
       cout<<"Error: "<<WSAGetLastError()<<endl;
       return;
      }
      if(answer.command!=61)
          finish=true;
      else
          fwrite(answer.script,strlen(answer.script),1,in);
    }
    fclose(in);
    delete[] pBuff;
    return;
}


void readCommands()
{
    struct timeval tv;
     tv.tv_sec = 0;
     tv.tv_usec = 0;
    fd_set readfds;
    _client *my_client=(_client*) client;
    SOCKET monitoringSock=my_client->monitoring_sock;
    //while(!global_quit)
    //{
          //������� readfds
          FD_ZERO(&readfds);
          //������� ���������� ������ � readfds
          FD_SET(monitoringSock,&readfds);
          //��������� �������� - ����� ��������. ���������� ���� �����
          //Select �� ���������� ���������� ��������� �� ����� ��������� ������
          select(NULL,&readfds,NULL,NULL,&tv);
          //���� ������ ������ �� ������ �� ������
          int bytes_recv;
          char tmp[STR_SIZE];
          if(FD_ISSET(monitoringSock,&readfds))
          {
               //��������� �� ���������� �� ������
               char *pBuff = new char[STR_SIZE];
               if((bytes_recv=recv(monitoringSock,pBuff,sizeof( sendStruct),0)) &&(bytes_recv!=SOCKET_ERROR))
               {
                   sendStruct receivedCommand;
                   memcpy( &receivedCommand, pBuff, sizeof( sendStruct));
                   previousState=currentState;
                   currentState=receivedCommand.command;
               }
               delete[] pBuff;
          }
    //}
}

void sendMonitoring(int type,char text[STR_SIZE], char arbiter_id[STR_SIZE], char arbiter_parent[STR_SIZE]) //������� ��������� ��� �����������
{
    if(currentState>=1)
    {
        receiveStruct send_struct;
        send_struct.command=type;
        strcpy(send_struct.text,text);
        strcpy(send_struct.arbiter_id,arbiter_id);
        strcpy(send_struct.arbiter_parent,arbiter_parent);
        char *pBuff = new char[sizeof(receiveStruct)];
        memcpy(pBuff,&send_struct,sizeof(receiveStruct));
        send(client->monitoring_sock,pBuff, sizeof(receiveStruct), 0);
        delete[] pBuff;
    }
}

void readSocket(void *client)
{
     char *part_str;
     char buf[STR_SIZE]; //������ ��������� ������
     struct timeval tv;
     tv.tv_sec = 0;
     tv.tv_usec = 0;
     fd_set readfds;
     bool quit=false;
     _client *my_client=(_client*) client;
     SOCKET my_sock=my_client->my_sock;
     SOCKET monitoringSock=my_client->monitoring_sock;
     dispatcher_answer answer;
     while(!global_quit)
     {
        //��������� �������� select
        //�-� ��������� ������ ������ my_sock.
        //��������� �������� 0 - �-� select �� ������������
        //������ ���������, � ������ ���� ��� ������ ��������� ������
        Sleep(1);
        bool fl=false;

        while(!quit)
        {
            fl=false;
            Sleep(1);
            //������� readfds
            FD_ZERO(&readfds);
            //������� ���������� ������ � readfds
            FD_SET(my_sock,&readfds);
            while(!fl)
            {
                readCommands();
                stopWaitForContinue();
                if(currentState==4)
                {
                    save();
                    currentState=2;
                    stopWaitForContinue();
                }

               //��������� �������� - ����� ��������. ���������� ���� �����
               //Select �� ���������� ���������� ��������� �� ����� ��������� ������
               select(NULL,&readfds,NULL,NULL,&tv);
               //���� ������ ������ �� ������ �� ������
               int bytes_recv;
               char tmp[STR_SIZE];
               if(FD_ISSET(my_sock,&readfds))
               {
                    //��������� �� ���������� �� ������
                    char *pBuff = new char[sizeof(dispatcher_answer)];
                    if((bytes_recv=recv(my_sock,pBuff,sizeof(dispatcher_answer),0)) &&(bytes_recv!=SOCKET_ERROR))
                    {
                        memcpy(&answer,pBuff,sizeof(dispatcher_answer));
                        //cout<<answer.actor_behavior<<endl;
                        char text[STR_SIZE];
                        switch(answer.command)
                        {
                            case 1: // Create
                                actors[answer.arbiter_id].behavior=string(answer.actor_behavior);
                                actors[answer.arbiter_id].count=answer.actor_par_count;
                                for(int i=0;i<answer.actor_par_count;i++)
                                    strcpy(actors[answer.arbiter_id].parameters[i],answer.actor_parameters[i]);
                                init_lua(scriptFileName,actors[answer.arbiter_id].luaVM); // �������������� ������������� ��� ����������� ������
                                // ��������� index ������ � ���������� ���������� self
                                lua_pushstring(actors[answer.arbiter_id].luaVM, answer.arbiter_id);
                                lua_setglobal(actors[answer.arbiter_id].luaVM, "self");
                                self_setted=true;


                                strcpy(text,"create: ");
                                strcat(text,answer.arbiter_id);
                                sendMonitoring(0,text,answer.arbiter_id,answer.arbiter_parent);


                                break;
                            case 2: // Send
                                send_actor_obr(answer);

                                strcpy(text,"send: ");
                                strcat(text,answer.arbiter_id);
                                for(int i=0;i<answer.actor_par_count;i++)
                                {
                                    strcat(text," ");
                                    strcat(text,answer.actor_parameters[i]);
                                }
                                sendMonitoring(1,text,answer.arbiter_id,answer.arbiter_parent);
                                break;
                            case 3: // become
                                /*actors[answer.arbiter_id].behavior=answer.actor_behavior;
                                actors[answer.arbiter_id].count=answer.actor_par_count;
                                for(int i=0;i<answer.actor_par_count;i++)
                                    strcpy(actors[answer.arbiter_id].parameters[i],answer.actor_parameters[i]);*/

                                strcpy(text,"become: ");
                                strcat(text,answer.actor_behavior);
                                sendMonitoring(2,text,answer.arbiter_id,answer.arbiter_id);

                                break;
                            case 6: //�������������� �������� �������
                                recv_file(my_sock,answer.worker_id); // ����� ��� ����� ��������� � ���������� scriptFileName
                                init_lua(scriptFileName,g_LuaVM);
                                break;
                            case 7: //������������� �������(����� ����������)
                                start_lua("createAndInitActors"); //������ �������� � ������� creatAndInitActors
                                break;
                            case 10:
                                quit=true;
                                global_quit=true;
                                fl=true;
                                break;

                        }
                    }
                    delete[] pBuff;
               }
               else
                   fl=true;
            }
        }
        answer.command=10;
        char *pBuff = new char[sizeof(dispatcher_answer)];
        memcpy(pBuff,&answer,sizeof(dispatcher_answer));
        send(my_client->my_sock,pBuff, sizeof(dispatcher_answer), 0);

    }
    closesocket(my_client->my_sock);
    closesocket(my_client->monitoring_sock);
    delete my_client;

}

char *_client::getCin()
{
    _flushall();
    char buf[STR_SIZE],*p; //������ ��������� ������
    cin.getline(buf,STR_SIZE);
    //++++������� ������ cin
    cin.clear();
    p=buf;
    return p;
}

char *_client::generateArbiterId()
{
    char p[STR_SIZE];
    char tmp[STR_SIZE];
    strcpy(p,client_id);
    strcat(p,";");
    sprintf(tmp,"%d",arbiter_num);
    strcat(p,tmp);
    arbiter_num++;
    return p;
}
//////////////////////////////

void serializeActorsForSave(saveActor *saveActorsArray)
{
    int i=0;
    for(std::map <string,actor> ::iterator it = actors.begin(); it != actors.end(); ++it)
    {
        strcpy(saveActorsArray[i].behavior,(*it).second.behavior.c_str());
        for(int j=0;j<5;j++)
        {
            strcpy(saveActorsArray[i].parameters[j],(*it).second.parameters[j]);
        }
        saveActorsArray[i].count=(*it).second.count;
        strcpy(saveActorsArray[i].id,(*it).first.c_str());
        i++;
    }
}

void sendSaveStruct(saveActor *saveActorsArray, int totalSaveCount, int count_unread_messages)
{
    // �������� �������
    for(int i=0;i<totalSaveCount;i++)
    {
        saveActorsArray[i].totalSaveCount=totalSaveCount;
        saveActorsArray[i].totalUnreadMessages=count_unread_messages;
        char *pBuff = new char[sizeof(saveActor)];
        memcpy(pBuff,&saveActorsArray[i],sizeof(saveActor));
        send(client->monitoring_sock,pBuff, sizeof(saveActor), 0);
        delete[] pBuff;
    }
    if(totalSaveCount==0) // ���� �� ������� �� ������ ������ �� ������� - ����� ����� ��������� ���-����� �����������
    {
        saveActorsArray[0].totalSaveCount=totalSaveCount;
        saveActorsArray[0].totalUnreadMessages=count_unread_messages;
        char *pBuff = new char[sizeof(saveActor)];
        memcpy(pBuff,&saveActorsArray[0],sizeof(saveActor));
        send(client->monitoring_sock,pBuff, sizeof(saveActor), 0);
        delete[] pBuff;
    }
}

dispatcher_answer receiveMessageUnread()
{
    dispatcher_answer *asnwer;
    fd_set readfds;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    SOCKET clientSocket=client->my_sock;
    //������� readfds
    FD_ZERO(&readfds);
    //������� ���������� ������ � readfds
    FD_SET(clientSocket,&readfds);
    //��������� �������� - ����� ��������. ���������� ���� �����
    //Select �� ���������� ���������� ��������� �� ����� ��������� ������
    select(NULL,&readfds,NULL,NULL,&tv);
    //���� ������ ������ �� ������ �� ������
    dispatcher_answer answer;
    if(FD_ISSET(clientSocket,&readfds))
    {
        int bytes_recv;
        //���� ������ �� ���������� � �� �������� ���������
        char *pBuff = new char[sizeof(dispatcher_answer)];
        if((bytes_recv=recv(clientSocket,pBuff,sizeof(dispatcher_answer),0)) &&(bytes_recv!=SOCKET_ERROR))
        {
            memcpy( &answer, pBuff, sizeof( dispatcher_answer));
        }
        else //������ ����������
        {
            answer.command=-1;
        }
        delete[] pBuff;
    }
    else
        answer.command=-1;

    return answer;
}

void getUnreadMessages(dispatcher_answer *unreadMessages, int &count_unread_messages)
{
    // ��������� ��� ��� ���� �� ������� �� ���� �������� - ��� ����������. ����� Load ��� ��� ������ ����� ������������.
    // �� ������ �� �������� ������� - ������� 10 ���������� ����� ���������� ����������.
    dispatcher_answer received_answer;
    int count_read=0;
    while(count_read<=10)
    {
        received_answer=receiveMessageUnread();
        if(received_answer.command!=-1)
        {
            count_read=0;
            unreadMessages[count_unread_messages]=received_answer;
            count_unread_messages++;
        }
        count_read++;
    }
}

void sendUnreadMessages(dispatcher_answer *unreadMessages, int count_unread_messages)
{
    // �������� ������������� ���������
    for(int i=0;i<count_unread_messages;i++)
    {
        char *pBuff = new char[sizeof(dispatcher_answer)];
        memcpy(pBuff,&unreadMessages[i],sizeof(dispatcher_answer));
        send(client->monitoring_sock,pBuff, sizeof(dispatcher_answer), 0);
        delete[] pBuff;
    }
}


void stopWaitForContinue() // ������� ��������������� ���������� ��������� � ��������� �������.
{
    while(currentState==2)
    {
        readCommands();
    }
    if(currentState==3)  // ���� ������� �� ��������� ���, �� ����� ������ ���������, �� ���� ��������� ������� ���������� ������
        currentState=2;
}

void sendSaveCommand()
{
    //������� ��������� ��� ��������
    dispatcher_answer answer;
    answer.command=9;
    strcpy(answer.arbiter_id,"");
    strcpy(answer.actor_behavior,"");
    answer.actor_par_count=0;

    char *pBuff = new char[sizeof(dispatcher_answer)];
    memcpy( pBuff, &answer, sizeof(dispatcher_answer));
    send( client->my_sock, pBuff, sizeof(dispatcher_answer), 0 );
    delete[] pBuff;
}

void save()
{
    int count_unread_messages=0;
    dispatcher_answer unreadMessages[ARBITERS_MAX];

    getUnreadMessages(unreadMessages,count_unread_messages);

    saveActor *saveActorsArray=new saveActor[actors.size()];
    sendSaveCommand();

    serializeActorsForSave(saveActorsArray);
    sendSaveStruct(saveActorsArray,actors.size(),count_unread_messages);

    sendUnreadMessages(unreadMessages,count_unread_messages);

    currentState=previousState;
}

//++++Actor+++
//++++++++++++


bool LoadScript(char *script)
{

    int s = luaL_loadfile(g_LuaVM, scriptFileName);
  if(s != 0)
  {
    cout << "Error while loading script file!" << endl << "Error: " << lua_tostring(g_LuaVM, -1) << endl;

    return false;
  }

  // ��������� �����
  s = lua_pcall(g_LuaVM, 0, LUA_MULTRET, 0);

  return true;
}

int create_actor(lua_State *luaVM)
{
    // �������� ����� ���������� �� Lua ������� ����������
    int argc = lua_gettop(luaVM);
    actor act;
    // ���� ���������� ������ ������ - ���������� ������
    if(argc < 1)
    {
        cerr << "Create - wrong number of arguments!" << endl;
        // ������ 0 � Lua ������
        lua_pushnumber(luaVM, 0);

        // ���������� ������������ ��������
        return 1;
    }

    act.behavior=lua_tostring(luaVM,1);
    int count=0;
    for(int i=2;i<=argc;i++)   //� lua ��������� ���������� � 1, � ������ �� ��� �������, ������� i=2
    {
        if(!lua_isnil(luaVM,i))
            strcpy(act.parameters[i-2],lua_tostring(luaVM,i));
        else
            strcpy(act.parameters[i-2],"nil");
        count++;
    }
    act.count=count;

    char arbiterId[STR_SIZE];
    strcpy(arbiterId,client->generateArbiterId());

    char arbiter_self[STR_SIZE];
    lua_getglobal(luaVM,"self");
    int ind=lua_gettop( luaVM );
    int is_nil=lua_isnil(luaVM, ind);
    if(is_nil==0)
    {
        string index=lua_tostring( luaVM, ind);
        strcpy(arbiter_self,index.c_str());
    }
    else
    {
        strcpy(arbiter_self,"-1");
    }
    /*
    char text[STR_SIZE];
    strcpy(text,"create: ");
    strcat(text,arbiterId);

    sendMonitoring(0,text,arbiter_self);*/
    //stopWaitForContinue();

    sendAnswer(1,arbiterId,act,arbiter_self);

    // ���������� � Lua ������ ������ ���������� ������
    lua_pushstring(luaVM, arbiterId);


    return 1;
}
int send_actor(lua_State *luaVM)
{
    // �������� ����� ���������� �� Lua ������� ����������
    int argc = lua_gettop(luaVM);

    // ���� ���������� ������ ���� - ���������� ������
    if(argc < 2)
    {
        cerr << "Send - wrong number of arguments!" << endl;
        // ������ 0 � Lua ������
        lua_pushnumber(luaVM, 0);

        // ���������� ������������ ��������
        return 1;
    }
    char arbiterId[STR_SIZE];
    strcpy(arbiterId,lua_tostring(luaVM, 1)); //�������� index ������ �������� ��������(1-� �������� � send)
    int send_count=0;
    actor act;
    for(int i=2;i<=argc;i++)//
    {
        if(!lua_isnil(luaVM,i))
            strcpy(act.parameters[i-2],lua_tostring(luaVM,i));
        else
            strcpy(act.parameters[i-2],"nil");
        send_count++;
    }
    act.behavior="";
    act.count=send_count;

    char arbiter_self[STR_SIZE];
    lua_getglobal(luaVM,"self");
    int ind=lua_gettop( luaVM );
    int is_nil=lua_isnil(luaVM, ind);
    if(is_nil==0)
    {
        string index=lua_tostring( luaVM, ind);
        strcpy(arbiter_self,index.c_str());
    }
    else
    {
        strcpy(arbiter_self,"-1");
    }
    /*char text[STR_SIZE];
    strcpy(text,"send: ");
    strcat(text,arbiterId);
    sendMonitoring(1,text,arbiter_self);*/

    //stopWaitForContinue();

    sendAnswer(2,arbiterId,act,arbiter_self);

    return 0;
}

bool is_number(const std::string& s)
{
    std::string::const_iterator it = s.begin();
    while (it != s.end() && isdigit(*it)) ++it;
    return !s.empty() && it == s.end();
}
int send_actor_obr(dispatcher_answer answer)
{
    char arbiter_id[STR_SIZE];
    strcpy(arbiter_id,answer.arbiter_id);
    string func=actors[arbiter_id].behavior;
    actor act;
    //act.behavior=answer.actor_behavior;
    act.count=answer.actor_par_count;
    for(int i=0;i<answer.actor_par_count;i++)
        strcpy(act.parameters[i],answer.actor_parameters[i]);
    if(func=="final_print")//���������� �� ������, ��� ������� ����������
    {
        sendAnswer(5,arbiter_id,act,answer.arbiter_parent);
        actors.clear();
        client->arbiter_num=0;
    }
    else if(func=="print") //������ �����
    {
        sendAnswer(4,arbiter_id,act,answer.arbiter_parent);
    }
    else
    {
        // ����������� �� ������ ����� ������� ���������
        // ������� 2 ����: ��������� ��� ���������_���. ��� ���������� � send ������ ���������� (������ - ������ �� ������).
        // � get_actor_func_name ���������� ����� ����� �������, ���� ���� ������� ���������_���, �� ��� ������������ � func_name, ����� ���������
        // ���� spec_func_name ���������� true ���� ��� ������� �� ������������� ����� ������, �.�. ���������_���
        char func_name[100];
        bool spec_func_name=false;
        if(act.count!=0)
            get_actor_func_name(*func_name, spec_func_name, g_LuaVM,func.c_str(),act.parameters);
        else
            strcpy(func_name,func.c_str());

        lua_getglobal(actors[arbiter_id].luaVM, func_name); // ��� ����������� �� ������� ����� ������� ���������.
        //lua_isfunction(luaVM,lua_gettop(luaVM))
        //lua_pushstring(actors[arbiter_id].luaVM,arbiter_id); // self
        //������� �������� ��������� ������� ���� �������� � ������ ��� ��������
        for(int i=0;i<actors[arbiter_id].count;i++)
        {
            if(strcmp(actors[arbiter_id].parameters[i],"nil")==0)
                lua_pushnil(actors[arbiter_id].luaVM);
            else
            {
                    _lua_pushStringOrNumber(actors[arbiter_id].luaVM,actors[arbiter_id].parameters[i]);
            }
        }
        //����� �������� ��������� ��������� � �������� send
        for(int i=0;i<act.count;i++)
        {
            if(spec_func_name==true && i==0) // ���� ���������_���, �� ������ �������� ��� ���, ��� �� ��������.
                continue;
            if(strcmp(act.parameters[i],"nil")==0)
                lua_pushnil(actors[arbiter_id].luaVM);
            else
            {
                    _lua_pushStringOrNumber(actors[arbiter_id].luaVM,act.parameters[i]);
            }
        }

        if(spec_func_name==true)
            act.count--;
        if(lua_pcall(actors[arbiter_id].luaVM, actors[arbiter_id].count+(act.count), 0, 0) != 0)
        {
          // ��������� ������������ ��������,
          // ���� ��� �� 0, �������� �� ������
          // lua_tostring(g_LuaVM, -1) �������� �������� ������
          cerr << "Error calling function Send1: " << lua_tostring(actors[arbiter_id].luaVM, -1) << endl;
        }
    }
    return 0;
}

void get_actor_func_name(char &func_name, bool &spec_func_name, lua_State *luaVM, const char *str, char send_parameters[5][50])
{   // � ������� 2 ���� ������� ������������: ��������� ��� ���������_���. ������� ��������� ��� �� ������� ��� � ����������
    // � ���� �� ����� �������. ���� ���� ������������ ���������_���, ���� ��� ������������ ���������.
    // �� ��� ���� �������� ������ �������� � send_parameters

    // ��������� �������� �� ��������-�������� ������ ��������
    spec_func_name=false;
    bool alnum=true;
    for(int i=0;i<strlen(send_parameters[0]);i++)
    {
        if(!isalnum(send_parameters[0][i]))
            alnum=false;
    }
    if(!alnum)
        strcpy(&func_name,str);
    else
    {
        // ��������� ������� ������� � ����� ������
        char catstr[100];
        strcpy(catstr,str);
        strcat(catstr,"_");
        strcat(catstr,send_parameters[0]);
        lua_getglobal(luaVM, catstr);
        if(lua_isfunction(luaVM,lua_gettop(luaVM)))
        {
            strcpy(&func_name,catstr);
            spec_func_name=true;
        }
        else
            strcpy(&func_name,str);
    }
}

void _lua_pushStringOrNumber(lua_State *g_LuaVM, const char *str)
{
    int len=strlen(str);
    bool digit=true;
    for(int i=0;i<len;i++)
        if(!isdigit(str[i]))
            digit=false;
    if(digit)
        lua_pushnumber(g_LuaVM,atoi(str));
    else
        lua_pushstring(g_LuaVM,str);
}

int become_actor(lua_State *luaVM)
{
    actor act;
    // �������� ����� ���������� �� Lua ������� ����������
    int argc = lua_gettop(luaVM);
    // ���� ���������� ������ ���� - ���������� ������
    if(argc < 1)
    {
        cerr << "Create - wrong number of arguments!" << endl;
        // ������ 0 � Lua ������
        lua_pushnumber(luaVM, 0);

        // ���������� ������������ ��������
        return 1;
    }

    //����� self �� ���������� ��� ����� ������
    string behavior=lua_tostring(luaVM,1);
    lua_getglobal(luaVM,"self");
    string index=lua_tostring( luaVM, lua_gettop( luaVM ));
    actors[index].behavior=behavior;
    int count=0;
    for(int i=2;i<=argc;i++)   //� lua ��������� ���������� � 1, � ������ �� ��� �������, ������� i=2
    {
        if(!lua_isnil(luaVM,i))
            strcpy(actors[index].parameters[i-2],lua_tostring(luaVM,i));
        else
            strcpy(actors[index].parameters[i-2],"nil");
        count++;
    }
    actors[index].count=count;

    char arbiter_self[STR_SIZE];
    strcpy(arbiter_self,index.c_str());
    /*char text[STR_SIZE];
    strcpy(text,"become: ");
    strcat(text,behavior.c_str());
    sendMonitoring(2,text,arbiter_self,arbiter_self);

    stopWaitForContinue();
    if(currentState==4)
    {
        save();
        currentState=2;
        stopWaitForContinue();
    }*/
    if(currentState>0) // ���� ������� ���������� ���������� become
    {
        act.behavior=behavior;
        act.count=0;

        sendAnswer(3,arbiter_self,act,arbiter_self);
    }

    return 0;
}


void sendAnswer(int command, char arbiter_id[STR_SIZE], actor act, char arbiter_parent[STR_SIZE])
{
    //������� ��������� ��� ��������
    dispatcher_answer answer;
    answer.command=command;
    strcpy(answer.arbiter_id,arbiter_id);
    strcpy(answer.arbiter_parent,arbiter_parent);
    strcpy(answer.actor_behavior,act.behavior.c_str());

    answer.actor_par_count=act.count;
    for(int i=0;i<act.count;i++)
        strcpy(answer.actor_parameters[i],act.parameters[i]);

    char *pBuff = new char[sizeof(dispatcher_answer)];
    // ��������� sc
    memcpy( pBuff, &answer, sizeof(dispatcher_answer));
    send( client->my_sock, pBuff, sizeof(dispatcher_answer), 0 );
    delete[] pBuff;
}

static int lua_sleep(lua_State *L)
{
    int m = static_cast<int> (luaL_checknumber(L,1));
    Sleep(m*1000);
    // usleep takes microseconds. This converts the parameter to milliseconds.
    // Change this as necessary.
    // Alternatively, use 'sleep()' to treat the parameter as whole seconds.
    return 0;
}

bool init_lua(char *scriptFileName, lua_State *&loc_luaVM)
{
    // �������������� ���������
    loc_luaVM = lua_open();
    //
    lua_register(loc_luaVM, "create", create_actor);
    lua_register(loc_luaVM, "send", send_actor);
    lua_register(loc_luaVM, "become", become_actor);
    lua_register(loc_luaVM, "sleep", lua_sleep);

    int s = luaL_loadfile(loc_luaVM, scriptFileName);

    if(s!=0)
    {
        cout << "Error while loading script file!" << endl << "Error: " << lua_tostring(g_LuaVM, -1) << endl;
        return false;
    }

    // ��������� ������
    s = lua_pcall(loc_luaVM, 0, LUA_MULTRET, 0);

    return true;
}


void start_lua(char *func_name)
{
    //�������� main ������� lua �������
    // ����������� �� ������ ����� ������� onFileFound
    lua_getglobal(g_LuaVM, func_name);
    //lua_pushcfunction(g_LuaVM, lua_sleep);
    //lua_setglobal(g_LuaVM, "sleep");
    // ��������� ��������� ��������� � ���� ���� � ���������� ����� (fileName � �������)
    //lua_pushstring(g_LuaVM, strFilePath.c_str());

    // ������� ������� createAndInitActors
    lua_pcall(g_LuaVM, 0, 0, 0);
}

void clear()
{
    actors.clear();
    if(g_LuaVM!=NULL)
    {
        lua_close(g_LuaVM);
        remove(scriptFileName);
        g_LuaVM=NULL;
    }
}

//++++++++++++

int worker_main()
{
    // ��������� ��������� ��� ������� ���������
    char p_adr[30];
    /*if(argc==2)
       strcpy(p_adr,argv[1]);
    else*/
       strcpy(p_adr,SERVER_IP);
    client = new _client(p_adr);

    bool quit=client->connectToServer();

    _beginthread(readSocket,0,(void *)client); // ����� �� ������
    //_beginthread(readCommands,0,(void *)client);

    while(!global_quit)
    {
        if(client->readInput()) //������ � ����������
            global_quit=true;
        //Sleep(1);
    }
    delete client;
    return 1;
}
//--------------------------------------------------------
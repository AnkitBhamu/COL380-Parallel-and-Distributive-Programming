#include<bits/stdc++.h>
#include <fstream>
#include <iostream>
#include <chrono>
#include <mpi.h>
#include <iterator>
using namespace std;

map<pair<int,int>,int>edgessupport; 
 map<int,set<int>>adj2; 
 
  int totalcs;
  int my_rank;


void Filteredges(int k)
{
    stack<pair<int,int>>removeedges;
    map<pair<int,int>,bool>arr;
    for(auto it:edgessupport)
    {
        
        if(it.second<k-2)
        {
            removeedges.push(it.first);
            arr[it.first]=true;
            // cout<<"Pushing the edges "<<it.first.first<<" "<<it.first.second<<endl;
        }
        else
        arr[it.first]=false;
    }

    while(!removeedges.empty())
    {  
        pair<int,int>tmp=removeedges.top();
        removeedges.pop();
        
        if(tmp.first<tmp.second)
        { 
            int v1=tmp.first;
            int v2=tmp.second;
            for(auto &n :adj2[v2])
            {
                if(adj2[v1].count(n))
                {
                    edgessupport[{v1,n}]--;
                    edgessupport[{v2,n}]--;
                    edgessupport[{n,v1}]--;
                    edgessupport[{n,v2}]--;
                    if(edgessupport[{v1,n}]<k-2 && arr[{v1,n}]==false)
                    {
                        removeedges.push({v1,n});
                        removeedges.push({n,v1});
                        arr[{v1,n}]=true;
                        arr[{n,v1}]=true;
                    }
                    if(edgessupport[{v2,n}]<k-2 && arr[{v2,n}]==false)
                    {
                        removeedges.push({v2,n});
                        removeedges.push({n,v2});
                        arr[{v2,n}]=true;
                        arr[{n,v2}]=true;
                    }
                }
                adj2[tmp.first].erase(tmp.second);
                adj2[tmp.second].erase(tmp.first);
                edgessupport[{v1,v2}]=-1;
                edgessupport[{v2,v1}]=-1;
            
            }
            
        }
        
    }

}


int intersector(set<int>&a,set<int>&b)
{
  int count=0;
  for(auto &v:a)
  {
       set<int>::iterator temp=b.find(v);

       if(temp!=b.end())
         count++;

  }

return count;

}


void initialise(vector<pair<int,int>>&adj,int k)
{
    vector<int>supportalledges(adj.size(),0);
         
    int perprcs=(int)ceil(adj.size()/totalcs);
    vector<int>psupport;


    for(int i=my_rank;i<adj.size();i+=totalcs)
    { 
        pair<int,int>tmp=adj.at(i);
        set<int>s1=adj2[tmp.first];
        set<int>s2=adj2[tmp.second];    
        int count=intersector(s1,s2);
        psupport.push_back(count);
        s1.clear();
        s2.clear();
    }

        
    if(my_rank!=0)
        { 
            MPI_Send(psupport.data(),psupport.size(),MPI_INT,0,0,MPI_COMM_WORLD);
        }
    
    if(my_rank==0)
    {
        for(int i=1;i<totalcs;i++)
        {
            vector<int> rcv(perprcs+totalcs,0);
            
            MPI_Recv(rcv.data(),perprcs+totalcs,MPI_INT,i,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            
            for(int j=i;j<adj.size();j+=totalcs)
            {
                int index=(int)j/totalcs;
                supportalledges[j]=rcv[index];

            }
        }


        for(int j=0;j<adj.size();j+=totalcs)
        {
            supportalledges[j]=psupport[j/totalcs];
        }
    

        for(int i=0;i<adj.size();i++)
        {
            int v1=adj[i].first;
            int v2=adj[i].second;
            edgessupport[{v1,v2}]=supportalledges[i];
            edgessupport[{v2,v1}]=supportalledges[i];
        }

    }
    
psupport.clear();
supportalledges.clear();

return;

}



void prefilter(int startk)
{
    stack<int>del;

    vector<bool>arr(adj2.size(),false);

    for(int i=0;i<adj2.size();i++)
    {
        if(adj2[i].size()>0 && adj2[i].size()<startk-1)
        {
            del.push(i);
            arr[i]=true;
        }
    }

    while(del.size()>0)
    {   int vertex=del.top();

    // cout<<"Vertex deleted is "<<vertex<<endl;
        del.pop();

        if(!adj2[vertex].empty())
        { 
            for(auto &v:adj2[vertex])
            {  
                adj2[v].erase(vertex);
                edgessupport[{v,vertex}]=-1;
                edgessupport[{vertex,v}]=-1;
                if((adj2[v].size())<startk-1)
                {   
                    if(arr[v]==false)
                    { 
                        del.push(v);
                        arr[v]=true;
                    }
                }
            }
        }
        adj2.erase(vertex);

    }

arr.clear();

}

bool checktruss(int k)
{   
    if(my_rank==0)
    { 
        bool run=true;
        while(run)
        {  
            run=false;
            for(auto it:edgessupport)
            {   
                int v1=it.first.first;
                int v2=it.first.second;
                
                if(it.second==0)
                {
                    adj2[v1].erase(v2);
                    adj2[v2].erase(v1);
                    it.second--;

                }
                if(it.second>0 && it.second<k-2 && v1<v2)//If there is an edge required to be deleted
                {
                    run=true;
                    adj2[v1].erase(v2);
                    adj2[v2].erase(v1);
                    
                    for(auto &n :adj2[v2])
                    {
                    if(adj2[v1].count(n))
                        {
                            edgessupport[{v1,n}]--;
                            edgessupport[{v2,n}]--;
                            edgessupport[{n,v1}]--;
                            edgessupport[{n,v2}]--;
                        }
                    }
                    edgessupport[{v1,v2}]=0;
                    edgessupport[{v2,v1}]=0;
                }
            }
        }
        int totalconnected=0;

        // for(auto it:support)
        //     {
        //         if(it.S>0)
        //         {
        //             x1++;
        //         }
        //     }
        for(auto it:adj2)
        {
            totalconnected+=it.second.size();
        }

        if(totalconnected==0)
        return false;
        else
        return true; 
        
        }
    return false;

}
void path_finder(map<int,set<int>>&adj,int i,vector<bool>&visited,vector<int>&path)
{
    bool check =true;
    for(auto &v :adj[i])
    {
        if(visited[v]==false)
        {
            visited[v]=true;
            path.push_back(v);
            path_finder(adj,v,visited,path);
        }
    }
}


int main(int argc,char ** argv)
{


    int verbose=0;
    int taskid=atoi(argv[1]+9);
    string inputpath=argv[2]+12;
    string headerpath=argv[3]+13;
    string outputpath=argv[4]+13;
    verbose=atoi(argv[5]+10);
    int startk=atoi(argv[6]+9);
    int  endk=atoi(argv[7]+7);

    // std::cout<<"startk ::"<<startk<<endl;

    if(taskid==1)
    {
        auto start = std::chrono::high_resolution_clock::now();
        
        MPI_Init(NULL,NULL);
        MPI_Comm_size(MPI_COMM_WORLD,&totalcs);
        MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
        

           
            
            MPI_File p;
            MPI_File_open(MPI_COMM_SELF,inputpath.c_str(),MPI_MODE_RDONLY,MPI_INFO_NULL,&p);
            int n,m;
            MPI_File_read(p,&n,1,MPI_INT,MPI_STATUS_IGNORE);
            MPI_File_read(p,&m,1,MPI_INT,MPI_STATUS_IGNORE);

            for(int i=0;i<n;i++){
                int node,deg,x;
            

                MPI_File_read(p,&node,1,MPI_INT,MPI_STATUS_IGNORE);
                MPI_File_read(p,&deg,1,MPI_INT,MPI_STATUS_IGNORE);
                
                for(int j=0;j<deg;j++){
                    MPI_File_read(p,&x,1,MPI_INT,MPI_STATUS_IGNORE);
                    adj2[node].insert(x);
                }
            
            }
            MPI_File_close(&p);
        
            bool prevvalue=true;

            // prefilter(adj2,24);

        // for(auto it:adj2)
        // {
        //     cout<<it.first<<" "<<it.second.size()<<endl;
        // }
        // int x=count_edges();
        // cout<<"Number of edges "<<endl;
        
        ofstream file;
        if(my_rank==0)
        {
            file.open(outputpath);
        }
        for(int i=startk;i<=endk;i++)
        {  

            if(prevvalue==true)
            {
              
                prefilter(i+2);     
               
                if(i==startk)  
                {        
                    vector<pair<int,int>>edges;
                    std::map<int, set<int>>::iterator it = adj2.begin();
                    it = adj2.begin();

                    //for usee 

                    while(it != adj2.end())
                    { 
                        pair<int,int>tmp2;

                        int node=it->first;    
                        set<int>tmp=it->second;

                        for(auto &v :tmp)
                        {
                        if(node<v)
                        { tmp2.first=node;
                        tmp2.second=v;
                        edges.push_back(tmp2);
                        }
                        }        
                        ++it;
                    }   
                    initialise(edges,i+2);
                    edges.clear();
                } 
                
                if(my_rank==0)
                { 
                    bool value=false; 
                    value=checktruss(i+2); 
                    prevvalue=value;
                    file<<value<<endl;
                
                    
                    if(verbose==1 && value==true)
                    { vector<bool>temp1(n,false);
                    vector<int>temp2;
                    vector<vector<int>>temp3;

                    for(int i=0;i<n;i++)
                                {
                                if(temp1[i]==false)
                                {
                                    temp1[i]=true;
                                    temp2.push_back(i);
                                    path_finder(adj2,i,temp1,temp2);
                                    if(temp2.size()>2)
                                    temp3.push_back(temp2);
                                    temp2.clear();

                                }
                                }  
                
                    file<<temp3.size()<<endl;
                    for(int i=0;i<temp3.size();i++)
                    {
                        sort(temp3[i].begin(),temp3[i].end());
                        for(int j=0;j<temp3[i].size();j++)
                        {
                            file<<temp3[i][j]<<" ";
                        }
                            file<<endl;
                            temp3[i].clear();

                    }     
                        
                        temp3.clear();


                    }

                }



            

        }
        else
        {
            file<<"0"<<endl;
        }
        }
        if(my_rank==0)
        file.close();
        MPI_Finalize();
        return 0;
    }
}

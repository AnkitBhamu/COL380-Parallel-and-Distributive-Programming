#include <bits/stdc++.h>
#include <mpi.h>

using namespace std;

vector<int> reading_offsets;
map<int,set<int>> total_edges;
map<pair<int,int>,set<int>> supports;
set<pair<int,int>> edges_per_rank;

MPI_Datatype packet,pair_type;
MPI_File graphfile;

ifstream graphfile1;


char *inputfile;
char*header;
char *outfile;
int startk,endk;
int verbose;
int taskid;
int p;
int myrank;
int n,m;
int totalclusters;


struct Data
{
    int rank_have_edge;
    int edge_vertex1;
    int edge_vertex2;
    int delsupport;
   
};


void checkrcvdata(stack<pair<int,int>>&deletable,vector<Data>&recvdata,int k)
{
  for(int i=0;i<recvdata.size();i++)
      {     int edge_vertex2=recvdata[i].edge_vertex2;
            int edge_vertex1=recvdata[i].edge_vertex1;
            
        if(supports.find({edge_vertex1,edge_vertex2})!=supports.end())
          {
            set<int>edge_support=supports[{edge_vertex1,edge_vertex2}]; 
            int delsupport=recvdata[i].delsupport;
               
            if(edge_support.find(delsupport)!=edge_support.end())
             {   
                supports[{edge_vertex1,edge_vertex2}].erase(supports[{edge_vertex1,edge_vertex2}].find(delsupport));
             }

          int reduced_size=supports[{edge_vertex1,edge_vertex2}].size();
          
          if(reduced_size<k-2)
          {
              deletable.push({edge_vertex1,edge_vertex2});
          }

          edge_support.clear();

          }

      }

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



void  edges_collect()
{  
  vector<pair<int,int>>temp;
  int sendcount=0;
  int rcvcounts[totalclusters];
  temp.clear();

  std::map<pair<int,int>,set<int>>::iterator it=supports.begin();

  while(it!=supports.end())
   {
      if(it->second.size()>0)
      {
        sendcount++;
      }
    it++;
   }
  

  MPI_Gather(&sendcount,1,MPI_INT,rcvcounts,1,MPI_INT,0,MPI_COMM_WORLD);

  int rcvdisp[totalclusters];
  int sum1=0;
  

  vector<pair<int,int>>temp2;
  temp2.clear();


it=supports.begin();
while(it!=supports.end())
   {
      if(it->second.size()>0)
      {
        temp2.push_back(it->first);
      }
    it++;
   }



if(myrank==0)
{

    for(int i=0;i<totalclusters;i++)
    {
       sum1+=rcvcounts[i]; 
    }

    for(int i=0;i<sum1;i++)
    {  
       temp.push_back({0,0});    
    }

    
    rcvdisp[0]=0;

   for(int i=1;i<totalclusters;i++)
   {
    rcvdisp[i]=rcvdisp[i-1]+rcvcounts[i-1];
   }

  
 }

  MPI_Gatherv(temp2.data(),sendcount,pair_type,temp.data(),rcvcounts,rcvdisp,pair_type,0,MPI_COMM_WORLD);

  if(myrank!=0)
  {
    return;
  }


  total_edges.clear();
  
  for(int i=0;i<temp.size();i++)
    {
        total_edges[temp[i].first].insert(temp[i].second);
        total_edges[temp[i].second].insert(temp[i].first);
    }

}


bool pouchsort(Data p1,Data p2)
{
    return p1.rank_have_edge<p2.rank_have_edge;
}



void graph_distribute(vector<vector<int>>&fullgraph)
{
#pragma omp for
 for(int i=myrank;i<n;i+=totalclusters)
 {
   int temp1=(reading_offsets[i+1]-reading_offsets[i]-8)/4;

  vector<int>neighbors(temp1);
  MPI_File_read_at(graphfile,reading_offsets[i]+8,neighbors.data(),temp1,MPI_INT,MPI_STATUS_IGNORE);
     
   
   for(int j=0;j<temp1;j++)
   {  
    if(i<neighbors[j])
    {
      fullgraph[i].push_back(neighbors[j]);
    }

   }

}


}



void input()
{

int graphsize1;

MPI_Info info;
MPI_Info_create(&info);    
MPI_Info_set(info,"collective_buffering", "true");
MPI_File_open(MPI_COMM_WORLD,inputfile,MPI_MODE_RDONLY,info, &graphfile);

graphfile1.open(inputfile);

graphfile1.read((char*)&n,4);
graphfile1.read((char*)&m,4);


graphfile1.seekg(0, ios::end);
graphsize1=graphfile1.tellg();

graphfile1.close();

reading_offsets=vector<int>();

ifstream headerfile(header);

for(int i=0;i<n;i++)
{
  int x;
  headerfile.read((char *)&x,sizeof(int32_t));
  reading_offsets.push_back(x);
}

headerfile.close();

reading_offsets[n]=graphsize1;

vector<vector<int>> fullgraph;
fullgraph.resize(n);

  

graph_distribute(fullgraph);

#pragma omp for
 for(int i=0;i<n;i++)
 {
    for(int j=0;j<fullgraph[i].size();j++)
    {
        edges_per_rank.insert({i,fullgraph[i][j]});
    }
  
 }

fullgraph.clear();

}



void triangle_enumeration()
{
   for(auto &v:edges_per_rank)
   { int vertex1=v.first;
     int vertex2=v.second;

     vector<int> neighbors_v1( (reading_offsets[vertex1+1]-reading_offsets[vertex1]-8)/4);
     vector<int> neighbors_v2((reading_offsets[vertex2+1]-reading_offsets[vertex2]-8)/4);

     //now reading the neighbors from graph file of these two vertices::

     MPI_File_read_at(graphfile,reading_offsets[vertex1]+8,neighbors_v1.data(),(reading_offsets[vertex1+1]-reading_offsets[vertex1]-8)/4,MPI_INT,MPI_STATUS_IGNORE);
     MPI_File_read_at(graphfile,reading_offsets[vertex2]+8,neighbors_v2.data(),(reading_offsets[vertex2+1]-reading_offsets[vertex2]-8)/4,MPI_INT,MPI_STATUS_IGNORE);

    
    if(neighbors_v1.size()<=neighbors_v2.size())
    {  
      for(int i=0;i<neighbors_v2.size();i++)
      {
        for(int j=0;j<neighbors_v1.size();j++)
        {
          if(neighbors_v2[i]==neighbors_v1[j])
          {
            supports[{vertex1,vertex2}].insert(neighbors_v2[i]);
          }
        }
      }


    }


    if(neighbors_v1.size()>neighbors_v2.size())
    {  
        for(int i=0;i<neighbors_v1.size();i++)
        {
               for(int j=0;j<neighbors_v2.size();j++)
               {
                  if(neighbors_v1[i]==neighbors_v2[j])
                  {
                    supports[{vertex1,vertex2}].insert(neighbors_v1[i]);
                  }
               }
        }


    }

   }

   edges_per_rank.clear();

}


bool checkkmintruss(int k)
{ 
  stack<pair<int,int>>deletable;
  std::map<pair<int,int>,set<int>>::iterator it=supports.begin();

  while(it!=supports.end())
   {
      if(it->second.size()<k-2)
          deletable.push(it->first);
      it++;
   }
  

  bool per_rank_done=false;
  bool all_ranks_done=false;
  
  while(all_ranks_done==false)
  {
    int sendpackets[totalclusters]={0};
    int rcvpackets[totalclusters]={0};

    vector<Data>sendpacketsdata;

    while(deletable.empty()==false)
   {  
    
      pair<int,int>temp;
      temp=deletable.top();
      deletable.pop();

    int vertex1=temp.first;
    int vertex2=temp.second;

    //check wether it has  the removable pair or not in its triangles

    if(supports.find({vertex1,vertex2})!=supports.end())
    {
      
      //iterating over the  support for this triangle

      for(auto &v:supports[{vertex1,vertex2}])
      {
        sendpacketsdata.push_back({(min(vertex1,v))%totalclusters,min(vertex1,v),max(vertex1,v),vertex2});
        sendpackets[(min(vertex1,v))%totalclusters]++;

        sendpacketsdata.push_back({(min(vertex2,v))%totalclusters,min(vertex2,v),max(vertex2,v),vertex1});
        sendpackets[(min(vertex2,v))%totalclusters]++;
      }  
        
        supports.erase({vertex1,vertex2});
    }

   } 
        
        sort(sendpacketsdata.begin(),sendpacketsdata.end(),pouchsort);

        MPI_Alltoall(sendpackets,1,MPI_INT,rcvpackets,1,MPI_INT,MPI_COMM_WORLD);


        int senddisp[totalclusters]={0};
        int rcvdisp[totalclusters]={0};
        
        int i=1;
        while(i<totalclusters)
        {
          senddisp[i]=senddisp[i-1]+sendpackets[i-1];
          rcvdisp[i]=rcvdisp[i-1]+rcvpackets[i-1];
          i++;
        }
        

      int totalrcv=rcvdisp[totalclusters-1]+rcvpackets[totalclusters-1];

      vector<Data> recvdata(totalrcv+1);

      MPI_Alltoallv(sendpacketsdata.data(),sendpackets,senddisp,packet,recvdata.data(),rcvpackets,rcvdisp,packet,MPI_COMM_WORLD);
      
      //extracting the data::

       checkrcvdata(deletable,recvdata,k);
    

      per_rank_done=deletable.empty();
      MPI_Allreduce(&per_rank_done,&all_ranks_done,1,MPI_C_BOOL,MPI_LAND,MPI_COMM_WORLD);
  }
  int totalrem=0;
  
  it=supports.begin();
  while(it!=supports.end())
   {
      totalrem+=it->second.size();  
      it++;
   }
  

  if(totalrem==0)
  {
    return false;
  }

  else
  {
    return true;
  }

}



void task1()
{  
   ofstream checkfile;

    if(myrank==0)
    {
      checkfile.open(outfile);
    }


   for(int k=startk;k<=endk;k++)
    {
      bool check_per_rank;
      bool check_all_ranks;
      check_per_rank=checkkmintruss(k+2);
      MPI_Allreduce(&check_per_rank,&check_all_ranks,1,MPI_C_BOOL,MPI_LOR,MPI_COMM_WORLD);
     
    
    
     if(verbose==1 && check_all_ranks)
     {
       edges_collect();

       if(myrank==0)
        {  vector<bool>temp1(n,false);
           vector<int>temp2;
           vector<vector<int>>temp3;
           checkfile<<check_all_ranks<<endl;

           for(int i=0;i<n;i++)
            {
              if(temp1[i]==false)
                {
                  temp1[i]=true;
                  temp2.push_back(i);
                  path_finder(total_edges,i,temp1,temp2);
                  if(temp2.size()>2)
                  temp3.push_back(temp2);
                  temp2.clear();

                }
            }

    
       checkfile<<temp3.size()<<endl;
       
       for(int i=0;i<temp3.size();i++)
         {
           sort(temp3[i].begin(),temp3[i].end());
        
           for(int j=0;j<temp3[i].size();j++)
             { 
                checkfile<<temp3[i][j]<<" ";
             }
      
          checkfile<<endl;
          temp3[i].clear();
 
         }
  
       }

     }


    else if(verbose ==1 && check_all_ranks==false)
    {
      if(myrank==0)
        checkfile<<"0"<<endl;
    }

    else if(verbose==0)
    {
      if(myrank==0)
        checkfile<<check_all_ranks<<" ";
    }
     
    
    }

}

void task2()
{  
  bool check_per_rank;
  bool check_all_ranks;

  check_per_rank=checkkmintruss(endk+2);
  MPI_Allreduce(&check_per_rank,&check_all_ranks,1,MPI_C_BOOL,MPI_LOR,MPI_COMM_WORLD);
  edges_collect();

  if(myrank!=0)
    return;
    
  if(myrank==0)
  {
    ofstream checkfile;
    checkfile.open(outfile);

    if(check_all_ranks==false)
    {
    checkfile<<"0"<<endl;
    checkfile.close();
    return;
    }
    if(check_all_ranks==true)
    { 

      vector<bool>temp1(n,false);
      vector<int>temp2;
      map<int,int>diff_groups;
      int groupcount=1;
      
      for(int i=0;i<n;i++)
      {
        if(temp1[i]==false)
          {
            temp1[i]=true;
            temp2.push_back(i);
            path_finder(total_edges,i,temp1,temp2);
            if(temp2.size()>2)
            {  
                for(int j=0;j<temp2.size();j++)
                {
                    diff_groups[temp2[j]]=groupcount;
                }
              
              groupcount++;
            }
            temp2.clear();

          }
      }

      map<int,set<int>>connected_components;
      vector<int>neighbors;
      for(int i=0;i<n;i++)
      {
        int degree=(reading_offsets[i+1]-reading_offsets[i]-8)/4;
        neighbors.resize(degree);
        MPI_File_read_at(graphfile , reading_offsets[i]+8 , neighbors.data() ,degree , MPI_INT ,MPI_STATUS_IGNORE);
        for(int j=0;j<degree;j++)
        {
          if(diff_groups[neighbors[j]]>0)
            connected_components[i].insert(diff_groups[neighbors[j]]);
        }
      }

      vector<int>result;

      for(auto v:connected_components)
      {

        if(v.second.size()>=p)
        {
            result.push_back(v.first);
        }

      } 
  
    checkfile<<result.size()<<endl;

    if(verbose==1)
    { for(int i=0;i<result.size();i++)
      {
        checkfile<<result[i]<<" ";
        checkfile<<"\n";

          
        std::map<int,int>::iterator it=diff_groups.begin();
        while(it!=diff_groups.end())
         {
            if(connected_components[result[i]].count(it->second))
                checkfile<<it->first<<" ";
                it++;
         }

          checkfile<<"\n";
        
      }
    
    }

   else
   {
    for(int i=0;i<result.size();i++)
      {
        checkfile<<result[i]<<" ";   
      }
   }
  checkfile.close(); 

  }

}

}




int main(int argc,char **argv)
{
    taskid=atoi(argv[1]+9);
    inputfile=argv[2]+12;
    header=argv[3]+13;
    outfile=argv[4]+13;
    verbose=atoi(argv[5]+10);
    startk=atoi(argv[6]+9);
    endk=atoi(argv[7]+7);
    p=atoi(argv[8]+4);

     
    
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&totalclusters);
    MPI_Comm_rank(MPI_COMM_WORLD,&myrank);


  MPI_Datatype arr_types[4]={MPI_INT,MPI_INT,MPI_INT,MPI_INT};
  MPI_Datatype arr_types1[2]={MPI_INT,MPI_INT};

  int blockcount[4]={1,1,1,1};
  int blockcount1[2]={1,1};
  
  MPI_Aint offsets[4];
  MPI_Aint disp1[2];

  pair<int,int> e(0,0);
  Data p;

  p.rank_have_edge=0;
  p.edge_vertex1=0;
  p.edge_vertex2=0;
  

  MPI_Get_address(&e.first,disp1);
  MPI_Get_address(&e.second,disp1+1);

  

  disp1[1]-=disp1[0];
  disp1[0]-=disp1[0];

  offsets[0]=offsetof(Data,rank_have_edge);
  offsets[1]=offsetof(Data,edge_vertex1);
  offsets[2]=offsetof(Data,edge_vertex2);
  offsets[3]=offsetof(Data,delsupport);


  for(int i=3;i>=0;i--)	
     offsets[i]-= offsets[0];

     
  
    MPI_Type_create_struct(4,blockcount,offsets,arr_types,&packet);
    MPI_Type_commit(&packet);

    MPI_Type_create_struct(2,blockcount1,disp1,arr_types1,&pair_type);
    MPI_Type_commit(&pair_type);   

 
   //taking the input for graph and the other stufff
    input();

    //calculating the edges support for finding the mintruss this is done only one time::
    
   
    triangle_enumeration();
   

   

    if(taskid==1)
      task1();
    
    if(taskid==2)
      task2();


MPI_Finalize();
return 0;

}


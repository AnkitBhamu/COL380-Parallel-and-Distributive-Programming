#include <bits/stdc++.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
using namespace std;

struct blockm
{
    int i;
    int j;
    vector<short>block_value;
};

struct cords
{
  int i;
  int j;
};

struct outblocks
{
  int i;
  int j;
  vector<int>data;
};

__global__ void matrixMul(const short *a, const short *b, int *c,int n) {
  
  // Compute each thread's global row and column index
  int row = blockIdx.y * blockDim.y + threadIdx.y;
  int col = blockIdx.x * blockDim.x + threadIdx.x;

  long long temp = (long long)0;

  if(row< n && col < n)
  {   
      for(int i=0;i<n;i++)
      {
          temp+=(long long )a[row*n+i]*(long long )b[i*n+col];
      }

      if(temp>4294967295)
             temp=4294967295;

      c[row*n+col]=(unsigned int)temp;       
               
  }

}


void linearize(vector<vector<short>>&fmatrix,vector<blockm>&tblocks,int nz,int m)
{
 for(int i=0;i<nz;i++)
 {
    blockm temp=tblocks[i];

    int sri=temp.i;
    int csi=temp.j;
    int k=0;
    
    for(int g=sri*m;g<sri*m+m;g++)
    {
        for(int h=csi*m;h<csi*m+m;h++)
            {
                fmatrix[g][h]=temp.block_value[k];
                k++;
            }
    }

  }

}

void naive(short *p1,int *o,int n)
{
  
    for (int i = 0; i < n; i++) {
        for (int j = 0; j <n; j++) {
            //  rslt[i][j] = 0;
             o[i*n+j]=0;
  
            for (int k = 0; k < n; k++) {
                cout<<"value is following :: "<<p1[i*k+j]<<endl;
                o[i*n+j] += (int)p1[i*n+k] * (int)p1[i*k+j];
            }
        }
  
    }
}


int main(int argc, char* argv[])
{
  string fn1(argv[1]);
  string fn2(argv[2]);
  string ofn(argv[3]);

  // cout<<"f1 is "<<fn2<<endl;


int n,m;
int nz;
int nz2;

ifstream infile;
ifstream infile2;

infile.open(fn1,ios::binary);
infile2.open(fn2,ios::binary);

infile.read((char*)&n,4);
infile.read((char*)&m,4);
infile.read((char*)&nz,4);

infile2.read((char*)&n,4);
infile2.read((char*)&m,4);
infile2.read((char*)&nz2,4);


vector<blockm>tblocks;

for(int i=0;i<nz;i++)
{
    blockm temp;
    infile.read((char*)&temp.i,4);
    infile.read((char*)&temp.j,4);   

    for(int i=0;i<m*m;i++)
    {
        short value;
        infile.read((char*)&value,2);
        temp.block_value.push_back(value);
    }
    tblocks.push_back(temp);
}

vector<vector<short>>fmatrix(n,vector<short>(n,(short)0));
linearize(fmatrix,tblocks,nz,m);
tblocks.clear();


//taking input2::
for(int i=0;i<nz2;i++)
{
    blockm temp;
    infile2.read((char*)&temp.i,4);
    infile2.read((char*)&temp.j,4);   

    for(int i=0;i<m*m;i++)
    {
        short value;
        infile2.read((char*)&value,2);
        temp.block_value.push_back(value);
    }
    tblocks.push_back(temp);
}

vector<vector<short>>fmatrix2(n,vector<short>(n,(short)0));
linearize(fmatrix2,tblocks,nz2,m);
tblocks.clear();


short * a=(short*)malloc(n*n*sizeof(short));
short * b=(short*)malloc(n*n*sizeof(short));

int counter=0;
for(int i=0;i<fmatrix.size();i++)
{
    for(int j=0;j<fmatrix[0].size();j++)
    {
        a[counter]=fmatrix[i][j];
        b[counter]=fmatrix2[i][j];
        counter++;
    }
}

fmatrix.clear();
fmatrix2.clear();



short* input1;
short* input2;
int * out;

size_t bytes1 = n*n*sizeof(short);
size_t bytes2 = n*n*sizeof(int);

cudaMalloc(&input1,bytes1);
cudaMalloc(&input2,bytes1);
cudaMalloc(&out,bytes2);

 // Copy data to the device
  cudaMemcpy(input1, a,n*n*sizeof(short), cudaMemcpyHostToDevice);
  cudaMemcpy(input2, b,n*n*sizeof(short), cudaMemcpyHostToDevice);
 

  int BLOCK_SIZE = 32;
  int GRID_SIZE=(int)ceil((float)n/BLOCK_SIZE);

  dim3 threads(BLOCK_SIZE, BLOCK_SIZE);
  dim3 blocks(GRID_SIZE,GRID_SIZE);

  matrixMul<<<blocks, threads>>>(input1,input2,out,n);

  
  int * output=(int*)malloc(n*n*sizeof(int));
  cudaError_t err=cudaMemcpy(output, out,n*n*sizeof(int), cudaMemcpyDeviceToHost);  
  

  cudaFree(input1);
  cudaFree(input2);
  cudaFree(out);

//************************************DONE MULTIPLYING*************************************************************************************
  vector<vector<int>>matrix;
  vector<int>block11;

    for(int i=0;i<n*n;i++)
    { 
      if((i+1)%n==0)
       {
        // cout<<output[i];
        
        block11.push_back(output[i]);
        matrix.push_back(block11);
        block11.clear();
        
        // cout<<endl;
       }
    else 
      {
        // cout<<output[i]<<" ";
       
       block11.push_back(output[i]);
      }
    }

  free(output);
  

  ofstream outfile(ofn,ios::binary);
  outfile.write((char *)&n,sizeof(int));
  outfile.write((char *)&m,sizeof(int));


  vector<outblocks>output333;
    
    int block_size=m;

    for (int i = 0; i < matrix.size(); i += block_size) {
        for (int j = 0; j < matrix.size(); j += block_size) {
            vector<int> block;
            int count=0;
            
            for (int x = i; x < i + block_size; x++) {
                for (int y = j; y < j + block_size; y++) {
                    
                    int data=matrix[x][y];

                    if(data==0)
                          count++;
                    block.push_back(matrix[x][y]);
                }
            }
            int ind=i/m;
            int ind2=j/m;

            if(count==m*m)
                 {
                  continue;
                 }

            else
            {
              outblocks temp111;
              temp111.i=ind;
              temp111.j=ind2;

              for(int i=0;i<block.size();i++)
               {
                      int value=block[i];
                      temp111.data.push_back(value);

               }
              output333.push_back(temp111);
              block.clear();   
            }    
        }
    }


int sz=output333.size();

outfile.write((char *)&sz,sizeof(int));


for(int i=0;i<output333.size();i++)
{
     outfile.write((char *)&output333[i].i,sizeof(int));
     outfile.write((char *)&output333[i].j,sizeof(int));

     for(int  j=0;j<output333[i].data.size();j++)
     {
      int value=output333[i].data[j];
      outfile.write((char *)&value,sizeof(int));
     
     }

}

output333.clear();
outfile.close();
matrix.clear();

return 0;
             
}

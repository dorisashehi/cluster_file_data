const fs = require('fs');
const csvParser = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fastCsv = require('fast-csv');


const inputDataFile = 'data.csv';
const outputDataFile = 'clustered_data.csv';


// calculate distance between two positions
function calculateDistance(x1, y1, x2, y2) {
    
    return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
}

// convert ISO 8601 timestamp to UNIX timestamp 
function isoToUnixFormat(timestamp) {
    return new Date(timestamp).getTime();
}

// convert UNIX timestamp to ISO 8601 format
function unixToIsoFormat(unixTimestamp) {
    return new Date(unixTimestamp).toISOString();
}

// check if all timestamps in a cluster have the same value
function areCommonTimestamps(cluster) {
    const firstTimestamp = cluster[0].timestamp_id;
    return cluster.every(item => item.timestamp_id === firstTimestamp);
}

  
const clusterData = (data) => {
    
    const clusters = [];
    const threshold = 2; //max distance

    data.forEach(item => {
       
        let clustered = false;
       
        for (let cluster of clusters) {
            for (let element of cluster) { 
                if (calculateDistance(item.x_position, item.y_position, element.x_position, element.y_position) <= threshold) {
                    cluster.push(item); //put them in the same cluster
                    clustered = true;
                    break;
                }
                
            }
            if (clustered) break;
        }
        
        if (!clustered) { //create another cluster if can't be put in any of the clusters.
           
            clusters.push([item]);
            
        }
    
    })


    return clusters.map(cluster => {
       
        let f_timestamp;
        if (areCommonTimestamps(cluster)) {
            // all timestampsare the same, we use the common timestamp
            f_timestamp = cluster[0].timestamp_id;
           
        } else {
            //else take average of them
            const averageUnixTimestamp = cluster.reduce((acc, cur) => acc + isoToUnixFormat(cur.timestamp_id), 0) / cluster.length;
            f_timestamp = unixToIsoFormat(averageUnixTimestamp);
        }

        const f_id = Math.floor(Math.random() * 10000); // Random ID for the cluster
        
        const cluster_data = cluster.map(({x_position, y_position, sensor_id}) => ({x_position, y_position, sensor_id}));
        
        const f_u_id = cluster.find(item => item.unique_id !== '0')?.unique_id || '0'; // Find a unique ID if available
        
        return { f_timestamp, f_id, cluster_data: JSON.stringify(cluster_data), f_u_id };
    });


}

// Read data from CSV
const readDataFromFile = () => {
    const data = [];
    return new Promise((resolve, reject) => {
      fs.createReadStream(inputDataFile)
        .pipe(csvParser())
        .on('data', (row) => data.push(row))
        .on('end', () => resolve(data))
        .on('error', (error) => reject(error));
    });
};

// Write clustered data to CSV
const writeClustersToFile = (clusteredData) => {
    const csvWriter = createCsvWriter({
      path: outputDataFile,
      header: [
        {id: 'f_timestamp', title: 'F_TIMESTAMP'},
        {id: 'f_id', title: 'F_ID'},
        {id: 'cluster_data', title: 'CLUSTER_DATA'},
        {id: 'f_u_id', title: 'F_U_ID'}
      ]
    });
  
    return csvWriter.writeRecords(clusteredData);
  };

// Main function
async function saveDataToFile() {
    try {
      const data = await readDataFromFile();
      const clusteredData = clusterData(data);
      await writeClustersToFile(clusteredData);
      console.log('Data processed and saved successfully.');
    } catch (error) {
      console.error('Error processing data:', error);
    }
  }


  saveDataToFile();
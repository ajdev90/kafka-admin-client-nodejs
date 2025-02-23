const {Kafka} = require('@confluentinc/kafka-javascript').KafkaJS;
const admin = new Kafka().admin({'bootstrap.servers': 'my-kafka-broker:9092'});
let adminConnected = false;

const connect = async () => {
    try {
        await admin.connect();
        console.log(`kafka admin client connection complete`);
        adminConnected= true;
    } catch (error) {
        console.log(error);
        adminConnected= false;
    }
}

const disconnect = async () => {
    try {
        await admin.disconnect();
    } catch (error) {

    }
}

const listTopics = async () => {
    let topics = [];
    try {
        topics = await admin.listTopics();
        console.log(`topics=${topics}`);
    } catch (error) {
        console.log(error);
    }
    return topics;
}

const fetchTopicOffsets = async (topic, timestamp) => {
    let topicOffsets = {};
    try {
        if (timestamp)
            topicOffsets = await admin.fetchTopicOffsets(topic, timestamp);
        else
            topicOffsets = await admin.fetchTopicOffsets(topic);
        console.log(`topicOffsets=${JSON.stringify(topicOffsets)}`);
    } catch (error) {
        console.log(error);
    }
    return topicOffsets;
}

const fetchTopicMetadata = async (topics) => {
    let topicsMetadata = [];
    try {
        let fetchTopicMetaRequest = { topics };
        topicsMetadata = await admin.fetchTopicMetadata(fetchTopicMetaRequest);
        if (topicsMetadata)
            topicsMetadata = topicsMetadata.map(obj => { delete obj.topicId; return obj; })
        console.log(`topicsMetadata=${JSON.stringify(topicsMetadata)}`);
    } catch (error) {
        console.log(error);
    }
    return topicsMetadata;
}

const listGroups = async () => {
    let groups = [];
    try {
        groups = await admin.listGroups();
        console.log(`groups=${groups}`);
    } catch (error) {
        console.log(error);
    }
    return groups;
}

const describeGroups = async (groups) => {
    let response =[];
    try {
        //let groupRequest = { groups };
        //console.log(JSON.stringify(groupRequest));
        response = await admin.describeGroups(groups);
        console.log(`groups=${response}`);
    } catch (error) {
        console.log(error);
    }
    return response;
}

const deleteGroups = async (groupIds) => {
    let response =[];
    try {
        console.log(JSON.stringify(groupIds));
        response = await admin.deleteGroups(groupIds);
        console.log(`groups=${response}`);
    } catch (error) {
        console.log(error);
        response = error.message;
    }
    return response;
}

const fetchOffsets = async (groupId) => {
    let response =[];
    try {
        let fetchOffsetsRequest = {groupId}
        console.log(JSON.stringify(fetchOffsetsRequest));
        response = await admin.fetchOffsets(fetchOffsetsRequest);
        console.log(`response=${response}`);
    } catch (error) {
        console.log(error);
    }
    return response;
}

const deleteTopics = async (topics) => {
    let response = false;
    try {
        let deleteRequest = { topics };
        console.log(JSON.stringify(deleteRequest));
        await admin.deleteTopics(deleteRequest);
        response = true;
    } catch (error) {
        console.log(error);
        response = false;
    }
    return response;
}

const deleteTopicRecords = async (topic, partitions) => {
    let response ={};
    try {
        let deleteRequest = { topic,partitions };
        console.log(JSON.stringify(deleteRequest));
        response = await admin.deleteTopicRecords(deleteRequest);
    } catch (error) {
        console.log(error);
    }
    return response;
}

const createTopics = async (topics) => {
    let response = false;
    try {
        let createRequest = { topics };
        console.log(JSON.stringify(createTopics));
        response = await admin.createTopics(createRequest);
        console.log(`createTopics response : ${response}`);
    } catch (error) {
        console.log(error);
    }
    return response;
}

module.exports = {
    deleteTopics: deleteTopics,
    deleteTopicRecords: deleteTopicRecords,
    listTopics: listTopics,
    connect: connect,
    disconnect: disconnect,
    createTopics: createTopics,
    listGroups: listGroups,
    describeGroups: describeGroups,
    deleteGroups: deleteGroups,
    fetchOffsets: fetchOffsets,
    fetchTopicMetadata: fetchTopicMetadata,
    fetchTopicOffsets: fetchTopicOffsets
}

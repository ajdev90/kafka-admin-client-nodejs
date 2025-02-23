const express = require('express');
const adminClient = require('./admin-client');
adminClient.connect();
const bodyParser = require('body-parser')
const app = express();
app.use(bodyParser.urlencoded())
app.use(bodyParser.json())

const port = 3000;

app.get('/topic', async (req, res) => {
    let topics = await adminClient.listTopics();
    res.send(topics);
})

app.post('/topic/offsets', async (req, res) => {
    let topicName =  req.body.topic;
    let response = await adminClient.fetchTopicOffsets(topicName);
    res.send(response);
})

app.post('/topic/metadata', async (req, res) => {
    let topics =  req.body.topics;
    let response = await adminClient.fetchTopicMetadata(topics);
    res.send(response);
})

app.delete('/topic', async (req, res) => {
    let topics =  req.body.topics;
    let response = await adminClient.deleteTopics(topics);
    res.send(response);
})

app.delete('/topic/records', async (req, res) => {
    let topic = req.body.topic;
    let partitions = req.body.partitions;
    let response = await adminClient.deleteTopicRecords(topic, partitions);
    res.send(response);
})

app.post('/topic', async (req, res) => {
    let topics =  req.body.topics;
    let response = await adminClient.createTopics(topics);
    res.send(response);
})

app.get('/group', async (req, res) => {
    let topics = await adminClient.listGroups();
    res.send(topics);
})

app.delete('/group', async (req, res) => {
    let groupIds =  req.body.groupIds;
    let topics = await adminClient.deleteGroups(groupIds);
    res.send(topics);
})

app.post('/group/info', async (req, res) => {
    let groups =  req.body.groups;
    let response = await adminClient.describeGroups(groups);
    res.send(response);
})

app.delete('/group', async (req, res) => {
    let topics =  req.body.topics;
    let response = await adminClient.createTopics(topics);
    res.send(response);
})

app.post('/group/offset', async (req, res) => {
    let groupId =  req.body.groupId;
    let response = await adminClient.fetchOffsets(groupId);
    res.send(response);
})

app.listen(port, () => {
    console.log(`Example app listening on port ${port}`)
})

process.on('uncaughtException', (err, origin) => {
    console.log(`${err} and origin :${origin}`)
});

// new file called DogPicture.jsx
import React, { useEffect, useState, useContext } from 'react';
import UserContext from './user-context';
import { io } from 'socket.io-client';
import Card from 'react-bootstrap/Card';
import Button from 'react-bootstrap/Button';

const socket = io("ws://localhost:5000", { transports: ["websocket"] });

const AIChatResponse = (props) => {
  const [imageUrl, setImageUrl] = useState('');
  const [reqid, setReqID] = useState('');
  const [socketInstance, setSocketInstance] = useState("");
  const [loading, setLoading] = useState(true);
  const [chatresponse, setChatResponseData] = useState(null);
  const [sessionid, setSessionID] = useState('');
  const [funddetails, setFundaData] = useState('');
  const [buttonStatus, setButtonStatus] = useState(true);
  /*const [serverMessage, setServerMessage] = useState(null);*/
  let role = "";
  let url = "";
  let session_id = "";
  let querybody = "";
  const user = useContext(UserContext);
  useEffect(() => {
    console.log(props.state.messages);
    let len = props.state.messages.length
      socket.on("connect", () => setSessionID(socket.id));

      const handleData = (payload) => {
          console.log("payload -->", payload);
          console.log(payload.answer);
          setChatResponseData(payload);
          setFundaData(payload.funds_details);
          socket.off("data", handleData);
      };
      socket.on("data", handleData);

      socket.on("disconnect", (data) => {
          console.log(data);
      });

    if(user.role=="Ask Doc"){
              url="/jobportal/querydoc?login=";
              role="AskDoc";
              console.log(sessionid)
              session_id = props.state.messages[len-2].id+"user";
              querybody={"searchquery": props.state.messages[len-2].message,
              "context":props.state.messages,"sessionid":sessionid}
              }
          else{
             url="/jobportal/aichat?login=";
             role="AIChat";
             querybody={"messages": props.state.messages }
            }

           const fetchAIresponse = async () => {
            console.log(role);
            const response = await fetch(url+user.name+'&role='+role,{
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(querybody)
            })
            const data = await response.json();
            console.log(data.reqid);
            //setAIResponseData(data.airesponse);
            setReqID(data.reqid);

        }
        fetchAIresponse();

      return () => socket.off("data", handleData);

  }, []);

  return (
    <div>
    {chatresponse &&
      <Card style={{ width: '30rem' }} key="chatresponsemain">
      <Card.Body>
       <div className="airesponse">{chatresponse.answer}</div>
      </Card.Body>
    </Card>}
    {funddetails && funddetails.length >0 && <div>
    <h5> Latest snapshot of Funds/Stocks mentioned in answer:</h5>
    {funddetails.map(fundetail => (
      <Card style={{ width: '30rem' }}>
      <Card.Body>

       <div className="airesponse">{fundetail.fund_name}-${fundetail.total}</div>
      </Card.Body>
    </Card>))}
    </div>}
    </div>


  );
};

export default AIChatResponse;
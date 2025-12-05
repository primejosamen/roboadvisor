// App.js
import './index.css';
import { useState } from "react";
import styled from "styled-components";
import Chatbot from 'react-chatbot-kit';
import ActionProvider from './ActionProvider';
import MessageParser from './MessageParser';
import config from './config';
import Accounts from "./Accounts";
import AskDoc from "./AskDoc";
import UserContext from './user-context';
import 'react-chatbot-kit/build/main.css';
import logo from './img.png';
import './App.css';

const Tab = styled.button`
  font-size: 14px;
  color: black;
  padding: 10px 60px;
  cursor: pointer;
  opacity: 1;
  background: transparent;
  border: 0;
  outline: 0;
  ${({ active }) =>
    active &&
    `
    border-bottom: 2px solid var(--color-primary-600);
    opacity: 1;
    font-weight: bold;
  `}
`;

const ButtonGroup = styled.div`
  display: flex;
  gap: 0.5rem;
`;

const types = ["Ask Doc", "Accounts"];

function App() {
    const [active, setActive] = useState(types[0]);
    const [value, setValue] = useState("Tim");
    const [showBot, toggleBot] = useState(false);

    const handleChange = (event) => {
        setValue(event.target.value);
    };

    return (
        <UserContext.Provider value={{ name: value, role: active }}>
            <div className="dashboard-root">
                {/* HEADER / NAVBAR */}
                <header className="dashboard-header">
                    <nav className="navbar navbar-expand-lg w-100">
                        <div className="d-flex align-items-center w-100 justify-content-between">
                            {/* Logo izquierda */}
                            <div className="d-flex align-items-center gap-2">
                                <img src={logo} width="64" height="64" alt="Logo" />
                                <span className="fw-semibold">SkyBot Demo</span>
                            </div>

                            {/* Tabs centrales */}
                            <ButtonGroup>
                                {types.map((type) => (
                                    <Tab
                                        key={type}
                                        active={active === type}
                                        onClick={() => setActive(type)}
                                    >
                                        {type}
                                    </Tab>
                                ))}
                            </ButtonGroup>

                            {/* Login derecha */}
                            <div>
                                <label>
                                    Login:
                                    <select value={value} onChange={handleChange}>
                                        <option value="John">John</option>
                                        <option value="Gopi">Gopi</option>
                                        <option value="Tim">Tim</option>
                                    </select>
                                    <span className="label">&nbsp; Welcome: {value}</span>
                                </label>
                            </div>
                        </div>
                    </nav>
                </header>

                {/* MAIN DASHBOARD */}
                <main className="dashboard-main">
                    {/* HERO central (logo + textos, estilo nuevo) */}
                    <section className="dashboard-hero">
                        <img
                            src={logo}
                            alt="Logo de empresa"
                            className="dashboard-hero-logo"
                        />
                        <div className="dashboard-hero-divider" />
                        <h1 className="dashboard-hero-title">Asistente Financiero</h1>
                        <p className="dashboard-hero-subtitle">
                            Descubre el poder de la IA en las finanzas
                        </p>
                    </section>

                    {/* CONTENIDO (AskDoc / Accounts) */}
                    <section className="dashboard-content">
                        <div className="dashboard-content-inner">
                            {active === "Ask Doc" && <AskDoc />}
                            {active === "Accounts" && <Accounts />}
                        </div>
                    </section>
                </main>

                {/* VENTANA DEL CHAT (estilo nuevo que ya te pasé antes) */}
                {showBot && (
                    <div className="chat-financial-window">
                        <div className="chat-financial-window__header">
                            <div className="chat-financial-window__header-inner">
                                <h6 className="chat-financial-window__title">SkyBot</h6>
                                <div className="chat-financial-window__header-actions">
                                    <button
                                        className="chat-financial-icon-btn"
                                        onClick={() => toggleBot(false)}
                                        aria-label="Cerrar"
                                    >
                                        ✕
                                    </button>
                                </div>
                            </div>
                        </div>
                        <div className="chat-financial-window__body">
                            <div
                                id="chat-messages"
                                className="chat-financial-window__messages"
                            >
                                <Chatbot
                                    config={config}
                                    messageParser={MessageParser}
                                    actionProvider={ActionProvider}
                                />
                            </div>
                        </div>
                    </div>
                )}

                {/* BOTÓN flotante tipo ChatFinancialBtn */}
                {!showBot && (
                    <button
                        className="chat-financial-btn"
                        onClick={() => toggleBot(true)}
                        aria-label="Abrir chat"
                    >
                        <span className="chat-financial-btn__icon">🤖</span>
                    </button>
                )}
            </div>
        </UserContext.Provider>
    );
}

export default App;

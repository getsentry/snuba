import React, { useState } from "react";
import ReactDOM from 'react-dom';

import Header from "./header";
import Nav from './nav';
import Body from './body';
import { NAV_ITEMS } from './data';


const bodyStyle = {
	height: '100%',
	display: 'flex'
}

ReactDOM.render(<App />, document.getElementById('root'));

function App() {
	const [activeTab, setActiveTab] = useState(NAV_ITEMS[0].id);

	return <React.Fragment><Header /><div style={bodyStyle}><Nav active={activeTab} /><Body active={activeTab} /></div></React.Fragment>;
}

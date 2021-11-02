import React from 'react';
import { COLORS } from './theme';
import {NAV_ITEMS} from './data';


type NavProps = {
	active: string
}

function Nav(props: NavProps) {
	return <nav style={navStyle}>
		<ul style={ulStyle}>
			{NAV_ITEMS.map(item => <li key={item.id} style={item.id === props.active ? liStyleActive : liStyleInactive}>{item.display}</li>)}
		</ul>
	</nav>
}

const navStyle = {
	borderRight: '1px solid #cbcbcb',
	width: '250px'
}

const ulStyle = {
	listStyleType: 'none',
	margin: 0,
	padding: 0,
}

const liStyle = {
	padding: '20px'
}

const liStyleActive = {
	...liStyle,
	color: COLORS.NAV_ACTIVE_TEXT
}

const liStyleInactive = {
	...liStyle,
	color: COLORS.NAV_INACTIVE_TEXT
}


export default Nav;

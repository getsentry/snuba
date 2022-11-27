import { COLORS } from "../theme";

const containerStyle = {
  width: 1200,
  maxWidth: "100%",
};

const linkStyle = {
  cursor: "pointer",
  fontSize: 13,
  color: COLORS.TEXT_LIGHTER,
  textDecoration: "underline",
};

const paragraphStyle = {
  fontSize: 15,
  color: COLORS.TEXT_LIGHTER,
};

const buttonStyle = {
  padding: "2px 5px",
  marginRight: "10px",
};

const defaultOptionStyle = {
  color: COLORS.TEXT_INACTIVE,
};

export {
  buttonStyle,
  containerStyle,
  linkStyle,
  paragraphStyle,
  defaultOptionStyle,
};

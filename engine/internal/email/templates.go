package email

const baseStyle = `
	body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f9fafb; margin: 0; padding: 0; }
	.container { max-width: 600px; margin: 0 auto; padding: 40px 20px; }
	.card { background-color: #ffffff; border-radius: 12px; border: 1px solid #e5e7eb; padding: 40px; text-align: center; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); }
	.logo { width: 48px; height: 48px; margin-bottom: 24px; }
	h1 { color: #111827; font-size: 24px; font-weight: 700; margin: 0 0 16px; line-height: 1.2; }
	p { color: #4b5563; font-size: 16px; line-height: 1.5; margin: 0 0 24px; }
	.btn { display: inline-block; background-color: #e11d48; color: #ffffff !important; font-weight: 600; font-size: 16px; text-decoration: none; padding: 12px 24px; border-radius: 8px; margin-bottom: 24px; text-align: center; }
	.footer { margin-top: 32px; color: #9ca3af; font-size: 14px; text-align: center; }
	.text-sm { font-size: 14px; }
	.url-fallback { color: #6b7280; font-size: 12px; word-break: break-all; margin-top: 32px; padding-top: 24px; border-top: 1px solid #f3f4f6; text-align: left; }
`

const baseTemplate = `
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<style>` + baseStyle + `</style>
</head>
<body>
	<div class="container">
		<div class="card">
			<img src="{{.LogoURL}}" alt="Sankofa Logo" class="logo" />
			<h1>{{.Subject}}</h1>
			<p>{{.PreviewText}}</p>
			
			<a href="{{.ActionURL}}" class="btn">{{.ActionText}}</a>
			
			<div class="url-fallback">
				Can't click the button? Copy and paste this link into your browser:<br/>
				<a href="{{.ActionURL}}" style="color: #e11d48; text-decoration: underline;">{{.ActionURL}}</a>
			</div>
		</div>
		<div class="footer">
			&copy; Sankofa. All rights reserved.<br/>
			You're receiving this because you signed up or were invited to Sankofa.
		</div>
	</div>
</body>
</html>
`

// We use the same generic layout for all three emails right now,
// customized via TemplateData fields passed into them in email.go

var inviteTemplate = baseTemplate
var verificationTemplate = baseTemplate
var passwordResetTemplate = baseTemplate

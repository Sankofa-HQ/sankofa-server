package email

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"log"
	"os"

	"github.com/mailersend/mailersend-go"
	"github.com/resend/resend-go/v2"
)

// EmailManager handles sending emails, with Resend as primary and MailerSend as fallback.
type EmailManager struct {
	resendClient     *resend.Client
	mailersendClient *mailersend.Mailersend
	fromEmail        string
	fromName         string
	frontendURL      string
}

// NewManager creates a new EmailManager.
func NewManager() *EmailManager {
	resendKey := os.Getenv("RESEND_API_KEY")
	mailerSendKey := os.Getenv("MAILERSEND_API_KEY")

	fromEmail := os.Getenv("EMAIL_FROM_ADDRESS")
	if fromEmail == "" {
		fromEmail = "noreply@sankofa.dev"
	}

	fromName := os.Getenv("EMAIL_FROM_NAME")
	if fromName == "" {
		fromName = "Sankofa"
	}

	frontendURL := os.Getenv("FRONTEND_URL")
	if frontendURL == "" {
		frontendURL = "https://app.sankofa.dev"
	}

	var rc *resend.Client
	if resendKey != "" {
		rc = resend.NewClient(resendKey)
	} else {
		log.Println("⚠️ RESEND_API_KEY is not set. Primary email sending will be disabled.")
	}

	var mc *mailersend.Mailersend
	if mailerSendKey != "" {
		mc = mailersend.NewMailersend(mailerSendKey)
	} else {
		log.Println("⚠️ MAILERSEND_API_KEY is not set. Fallback email sending will be disabled.")
	}

	return &EmailManager{
		resendClient:     rc,
		mailersendClient: mc,
		fromEmail:        fromEmail,
		fromName:         fromName,
		frontendURL:      frontendURL,
	}
}

// SendEmail attempts to send an email via Resend, falling back to MailerSend on failure.
func (m *EmailManager) SendEmail(toEmail, toName, subject, htmlContent string) error {
	var lastErr error

	// 1. Try Resend (Primary)
	if m.resendClient != nil {
		params := &resend.SendEmailRequest{
			From:    fmt.Sprintf("%s <%s>", m.fromName, m.fromEmail),
			To:      []string{toEmail},
			Subject: subject,
			Html:    htmlContent,
		}

		_, err := m.resendClient.Emails.Send(params)
		if err == nil {
			log.Printf("📧 Email sent successfully via Resend to %s", toEmail)
			return nil
		}
		lastErr = fmt.Errorf("resend failed: %w", err)
		log.Printf("⚠️ Resend failed to send email to %s: %v. Attempting fallback.", toEmail, err)
	} else {
		lastErr = fmt.Errorf("resend client not configured")
	}

	// 2. Try MailerSend (Fallback)
	if m.mailersendClient != nil {
		ctx := context.Background()

		from := mailersend.From{
			Name:  m.fromName,
			Email: m.fromEmail,
		}

		to := []mailersend.Recipient{
			{
				Name:  toName,
				Email: toEmail,
			},
		}

		message := m.mailersendClient.Email.NewMessage()
		message.SetFrom(from)
		message.SetRecipients(to)
		message.SetSubject(subject)
		message.SetHTML(htmlContent)

		_, err := m.mailersendClient.Email.Send(ctx, message)
		if err == nil {
			log.Printf("📧 Email sent successfully via MailerSend (Fallback) to %s", toEmail)
			return nil
		}
		return fmt.Errorf("both primary and fallback email providers failed. Last error: %w", err)
	}

	// 3. Fallback to mock log if neither is configured
	if m.resendClient == nil && m.mailersendClient == nil {
		log.Printf("📧 MOCK EMAIL (No providers configured)\nTo: %s\nSubject: %s\nContent Length: %d", toEmail, subject, len(htmlContent))
		return nil
	}

	return lastErr
}

// FrontendURL returns the configured frontend URL for building links
func (m *EmailManager) FrontendURL() string {
	return m.frontendURL
}

// TemplateData holds common data for all email templates
type TemplateData struct {
	FrontendURL string
	Subject     string
	PreviewText string
	ActionURL   string
	ActionText  string
	OrgName     string
	InviterName string
	LogoURL     string
}

func (m *EmailManager) renderTemplate(tmplString string, data TemplateData) (string, error) {
	data.FrontendURL = m.frontendURL
	data.LogoURL = fmt.Sprintf("%s/logo-icon.png", m.frontendURL) // Ensure you have a PNG logo in public folder

	t, err := template.New("email").Parse(tmplString)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// SendInviteEmail sends an organization invitation email
func (m *EmailManager) SendInviteEmail(toEmail, inviterName, orgName, inviteToken string) error {
	actionURL := fmt.Sprintf("%s/invite?token=%s", m.frontendURL, inviteToken)
	subject := fmt.Sprintf("You've been invited to join %s on Sankofa", orgName)

	data := TemplateData{
		Subject:     subject,
		PreviewText: fmt.Sprintf("%s has invited you to collaborate in %s.", inviterName, orgName),
		ActionURL:   actionURL,
		ActionText:  "Accept Invitation",
		OrgName:     orgName,
		InviterName: inviterName,
	}

	html, err := m.renderTemplate(inviteTemplate, data)
	if err != nil {
		return fmt.Errorf("failed to render invite template: %w", err)
	}

	return m.SendEmail(toEmail, "", subject, html)
}

// SendVerificationEmail sends an email address verification link
func (m *EmailManager) SendVerificationEmail(toEmail, verifyToken string) error {
	actionURL := fmt.Sprintf("%s/verify-email?token=%s", m.frontendURL, verifyToken)
	subject := "Verify your email address for Sankofa"

	data := TemplateData{
		Subject:     subject,
		PreviewText: "Please verify your email address to secure your Sankofa account.",
		ActionURL:   actionURL,
		ActionText:  "Verify Email Address",
	}

	html, err := m.renderTemplate(verificationTemplate, data)
	if err != nil {
		return fmt.Errorf("failed to render verification template: %w", err)
	}

	return m.SendEmail(toEmail, "", subject, html)
}

// SendPasswordResetEmail sends a password reset link
func (m *EmailManager) SendPasswordResetEmail(toEmail, resetToken string) error {
	actionURL := fmt.Sprintf("%s/reset-password?token=%s", m.frontendURL, resetToken)
	subject := "Reset your Sankofa password"

	data := TemplateData{
		Subject:     subject,
		PreviewText: "We received a request to reset your password. Click to create a new one.",
		ActionURL:   actionURL,
		ActionText:  "Reset Password",
	}

	html, err := m.renderTemplate(passwordResetTemplate, data)
	if err != nil {
		return fmt.Errorf("failed to render password reset template: %w", err)
	}

	return m.SendEmail(toEmail, "", subject, html)
}
